use crate::net::nats_tcp_stream::NatsTcpStream;
use crate::ops::{Op, Connect, Subscribe, Message, UnSubscribe, Publish};
use futures::{StreamExt, SinkExt};
use crate::nats_client::{NatsClientOptions, NatsClientInner, NatsSid, ClosableMessage, NatsClientState};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use crate::error::RatsioError;
use futures_timer::Delay;
use std::time::Duration;

use tokio::{
    net::TcpStream,
    sync::mpsc::UnboundedReceiver,
};
use futures::{
    stream::SplitStream,
};
use std::pin::Pin;
use futures::stream::Stream;
use std::task::{Context, Poll};
use crate::ops::Op::UNSUB;
use pin_project::pin_project;


impl NatsClientInner {
    //Establish tcp connection with one of the Nats servers
    // Process - go through uris in opts.cluster_uris, trying one at a time.
    //     If none of attempted connections succeed, wait and try again.
    pub(in crate::nats_client) async fn try_connect(opts: NatsClientOptions, cluster_uris: &Vec<String>, keep_retrying: bool) -> Result<TcpStream, RatsioError> {
        let valid_addresses = cluster_uris.iter().flat_map(|raw_uri| {
            let uri = if raw_uri.starts_with("nats://") {
                (&raw_uri[7..]).to_string()
            } else {
                raw_uri.clone()
            };
            match uri.parse::<SocketAddr>() {
                Ok(addr) => Some((uri, addr)).into_iter().collect::<Vec<_>>(),
                Err(_err) => {
                    match uri.to_socket_addrs() {
                        Ok(ips_iter) => ips_iter.map(|x| {
                            (uri.clone(), x)
                        }).collect::<Vec<_>>(),
                        Err(err) => {
                            error!("Unable resolve url => {} to ip address => {}", &uri, err);
                            Default::default()
                        }
                    }
                }
            }
        }).collect::<Vec<_>>();
        if valid_addresses.len() == 0 {
            return Err(RatsioError::GenericError("No valid NATS uris".into()));
        }
        loop {
            for uri_and_addr in valid_addresses.clone() {
                let (uri, addr) = uri_and_addr;
                match tokio::net::TcpStream::connect(&addr).await {
                    Ok(tcp_stream) => return Ok(tcp_stream),
                    Err(err) => {
                        error!("Error connecting to {} - {:?}", uri, err);
                    }
                }
            }
            error!("Unable to connect to any of the Nats servers, will retry again.");
            if keep_retrying {
                let _ = Delay::new(Duration::from_millis(opts.reconnect_timeout)).await;
            }else{
                return Err(RatsioError::NoRouteToHostError);
            }
        }
    }


    // Issue a connect command to NATS
    pub(in crate::nats_client) async fn start(self_arc: Arc<Self>, version: u128, mut stream: SplitStream<NatsTcpStream>) -> Result<(), RatsioError> {
        let opts = self_arc.opts.clone();
        //Register for NATS incoming messages
        //let mut executor = futures::executor::LocalPool::new();
        let stream_self = self_arc.clone();
        let _ = tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                let current_version = stream_self.reconnect_version.read().await;
                if *current_version != version {
                    break;
                }
                stream_self.process_nats_event(item).await
            }
        });
        //executor.run();
        let connect = Op::CONNECT(Connect {
            verbose: opts.verbose,
            pedantic: opts.pedantic,
            tls_required: opts.tls_required,
            auth_token: Some(opts.auth_token).filter(|a| !a.is_empty()),
            user: Some(opts.username).filter(|a| !a.is_empty()),
            pass: Some(opts.password).filter(|a| !a.is_empty()),
            name: Some(opts.name).filter(|a| !a.is_empty()),
            lang: "rust".to_string(),
            version: "0.3.0".to_string(),
            protocol: 1,
            echo: true,
            sig: None,
            jwt: None,
        });
        self_arc.send_command(connect).await?;
        let mut state_guard = self_arc.state.write().await;
        *state_guard = NatsClientState::Connected;
        Ok(())
    }

    pub(in crate::nats_client) fn time_in_millis() -> u128 {
        use std::time::SystemTime;
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_millis(),
            Err(_) => 0,
        }
    }

    pub(in crate::nats_client) async fn process_nats_event(&self, item: Op) {
        self.ping_pong_reset().await;
        match item {
            Op::CLOSE => {
                let _ = self.stop().await;
            }
            Op::INFO(server_info) => {
                let mut info = self.server_info.write().await;
                *info = Some(server_info)
            }
            Op::PING => {
                match self.send_command(Op::PONG).await {
                    Err(err) => {
                        error!(" Error sending PONG to Nats {:?}", err);
                    }
                    _ => {}
                }
            }
            Op::MSG(message) => {
                let subscriptions = self.subscriptions.lock().await;
                if let Some((sender, _)) = subscriptions.get(&message.sid) {
                    match sender.send(ClosableMessage::Message(message)) {
                        Err(err) => {
                            error!("Unable to send message to subscription - {:?}", err);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    pub(in crate::nats_client) async fn ping_pong_reset(&self) {
        let mut last_ping = self.last_ping.write().await;
        *last_ping = Self::time_in_millis();
    }

    pub(in crate::nats_client) async fn subscribe(
        &self,
        cmd: Subscribe,
    ) -> Result<(NatsSid, impl Stream<Item=Message> + Send + Sync), RatsioError> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let sid = if cmd.sid.is_empty() {
            crate::nuid::next()
        } else {
            cmd.sid.clone()
        };
        let mut subscriptions = self.subscriptions.lock().await;
        subscriptions.insert(sid.clone(), (sender, cmd.clone()));
        let _ = self.send_command(Op::SUB(cmd)).await?;
        Ok((NatsSid(sid), NatsClosableReceiver(receiver)))
    }

    pub(in crate::nats_client) async fn un_subscribe(
        &self,
        sid: NatsSid,
    ) -> Result<(), RatsioError> {
        let mut subscriptions = self.subscriptions.lock().await;
        match subscriptions.remove(&sid.0) {
            Some((sender, _)) => {
                let _ = sender.send(ClosableMessage::Close);
                let cmd = UNSUB(UnSubscribe {
                    sid: sid.0.clone(),
                    ..Default::default()
                });
                let _ = self.send_command(cmd).await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(in crate::nats_client) async fn publish(
        &self,
        cmd: Publish,
    ) -> Result<(), RatsioError> {
        self.send_command(Op::PUB(cmd)).await
    }

    pub(in crate::nats_client) async fn request(
        &self,
        mut cmd: Publish,
    ) -> Result<Message, RatsioError> {
        let reply_to = crate::nuid::next();
        cmd.reply_to = Some(reply_to.clone());

        let subscribe_command = Subscribe {
            subject: reply_to.clone(),
            sid: crate::nuid::next(),
            ..Default::default()
        };
        let (sid, mut subscription) = self.subscribe(subscribe_command).await?;
        let _ = self.send_command(Op::PUB(cmd)).await?;
        let response = subscription.next().await;
        let _ = self.un_subscribe(sid).await;
        match response {
            Some(message) => Ok(message),
            _ => { Err(RatsioError::RequestStreamClosed) }
        }
    }

    pub(in crate::nats_client) async fn stop(&self) -> Result<(), RatsioError> {
        let mut state_guard = self.state.write().await;
        *state_guard = NatsClientState::Shutdown;

        let mut reconnect = self.on_reconnect.lock().await;
        *reconnect = None;

        //Close all subscritions.
        let mut subscriptions = self.subscriptions.lock().await;
        for (sid, (sender, _)) in subscriptions.iter() {
            let _ = sender.send(ClosableMessage::Close);
            let cmd = UNSUB(UnSubscribe {
                sid: sid.clone(),
                ..Default::default()
            });
            let _ = self.send_command(cmd).await;
        }
        subscriptions.clear();
        let mut client_ref = self.client_ref.write().await;
        *client_ref = None;

        Ok(())
    }

    pub async fn reconnect(&self) -> Result<(), RatsioError> {
        let mut state_guard = self.state.write().await;
        if *state_guard == NatsClientState::Disconnected {
            *state_guard = NatsClientState::Reconnecting;
        } else {
            return Ok(());
        }

        match self.do_reconnect().await {
            Ok(_) => {
                let mut state_guard = self.state.write().await;
                *state_guard = NatsClientState::Connected;
                Ok(())
            }
            Err(err) => {
                error!("Error trying to reconnect to NATS {:?}", err);
                let mut state_guard = self.state.write().await;
                *state_guard = NatsClientState::Disconnected;
                Err(err)
            }
        }
    }

    async fn do_reconnect(&self) -> Result<(), RatsioError> {
        let client_ref_guard = self.client_ref.read().await;
        let client_ref = if let Some(client_ref) = client_ref_guard.as_ref() {
            client_ref.clone()
        } else {
            return Err(RatsioError::CannotReconnectToServer);
        };
        let tcp_stream = Self::try_connect(self.opts.clone(), &self.opts.cluster_uris.0, true).await?;
        let (sink, stream) = NatsTcpStream::new(tcp_stream).await.split();
        *self.conn_sink.lock().await = sink;
        let mut version = self.reconnect_version.write().await;
        let new_version = *version + 1;
        *version = new_version;
        info!("Reconnecting to NATS servers 4 - new version {}", new_version);
        let _ = NatsClientInner::start(client_ref.inner.clone(), new_version, stream).await?;
        if self.opts.subscribe_on_reconnect {
            let subscriptions = self.subscriptions.lock().await;
            for (_sid, (_sender, subscribe_command)) in subscriptions.iter() {
                match self.send_command(Op::SUB(subscribe_command.clone())).await {
                    Ok(_) => {
                        info!("re subscribed to => {:?}", subscribe_command.subject.clone());
                    }
                    Err(err) => {
                        info!(" Failed to resubscribe to => {:?}, reason => {:?}", subscribe_command.clone(), err);
                    }
                }
            }
        }
        client_ref.on_reconnect().await;
        Ok(())
    }

    async fn send_command(&self, cmd: Op) -> Result<(), RatsioError> {
        let mut conn_sink = self.conn_sink.lock().await;
        conn_sink.send(cmd).await
    }

    pub(in crate::nats_client) async fn monitor_heartbeat(&self) -> Result<(), RatsioError> {
        let ping_interval = u128::from(self.opts.ping_interval * 1000);
        let ping_max_out = u128::from(self.opts.ping_max_out);
        loop {
            let _ = Delay::new(Duration::from_millis((ping_interval / 2) as u64)).await;
            let state_guard = self.state.read().await;
            if *state_guard == NatsClientState::Shutdown {
                break;
            }

            let mut reconnect_required = false;
            match self.send_command(Op::PING).await {
                Ok(_) => {}
                Err(err) => {
                    error!("Error pinging NATS server {:?}", err);
                    reconnect_required = true;
                }
            }
            if !reconnect_required {
                let _ = Delay::new(Duration::from_millis((ping_interval / 2) as u64)).await;
                let now = Self::time_in_millis();
                let last_ping = self.last_ping.read().await;
                if now - *last_ping > ping_interval {
                    error!("Missed ping interval")
                }
                if (now - *last_ping) > (ping_max_out * ping_interval) {
                    reconnect_required = true;
                }
            }

            if reconnect_required {
                error!("Missed too many pings, reconnect is required.");
                let mut state_guard = self.state.write().await;
                *state_guard = NatsClientState::Disconnected;
                let _ = self.reconnect().await;
            }
        }
        Ok(())
    }
}

#[pin_project]
struct NatsClosableReceiver(#[pin] UnboundedReceiver<ClosableMessage>);

impl Stream for NatsClosableReceiver {
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().0.poll_recv(cx) {
            Poll::Ready(Some(ClosableMessage::Message(msg))) => {
                Poll::Ready(Some(msg))
            }
            Poll::Ready(Some(ClosableMessage::Close)) => {
                Poll::Ready(None)
            }
            Poll::Pending => {
                Poll::Pending
            }
            Poll::Ready(None) => {
                Poll::Ready(None)
            }
        }
    }
}




/*
pub struct NatsSubscriptionStream {
    waker: Arc<std::sync::Mutex<Option<Waker>>>,
    queue: Arc<std::sync::Mutex<VecDeque<Message>>>,
}

impl NatsSubscriptionStream {
    fn start(self, receiver: std::sync::mpsc::Receiver<Message>) -> Self {
        let queue = self.queue.clone();
        let waker = self.waker.clone();
        tokio::spawn(async move {
            for msg in receiver.iter() {
                if let Ok(mut queue_lock) = queue.lock() {
                    queue_lock.push_back(msg);
                }
                if let Ok(mut waker_lock) = waker.lock() {
                    if let Some(w) = waker_lock.as_ref() {
                        let w2 = w.clone();
                        //info!(" ------ waking up stream loop => {:?}", w2);
                        w2.wake();
                    }
                }
            }
        });
        self
    }
}

impl Stream for NatsSubscriptionStream {
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(mut queue) = self.queue.lock() {
            match queue.pop_front() {
                Some(msg) => Poll::Ready(Some(msg)),
                _ => {
                    if let Ok(mut waker_lock) = self.waker.lock() {
                        let w = cx.waker().clone();
                        //info!(" ------ register waker {:?}", w);
                        *waker_lock = Some(w);
                    }
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint = if let Ok( queue) = self.queue.lock() {
            (queue.len(), Some(queue.len() + 1))
        }else{
            (0, None)
        };
        hint
    }
}
*/