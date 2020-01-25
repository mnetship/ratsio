use crate::net::nats_tcp_stream::NatsTcpStream;
use crate::ops::{Subscribe, Message, Publish};
use futures::{StreamExt};
use crate::nats_client::{NatsClient, NatsClientOptions, NatsClientInner, NatsSid, ReconnectHandler, NatsClientState};

use std::sync::Arc;
use tokio::sync::RwLock;
use crate::error::RatsioError;

use futures::lock::Mutex;
use nom::lib::std::collections::{HashMap};
use futures::stream::Stream;


impl NatsClient {
    pub async fn new<O>(options: O) -> Result<Arc<Self>, RatsioError>
        where O: Into<NatsClientOptions> {
        let opts = options.into();
        let tcp_stream = NatsClientInner::try_connect(opts.clone(), &opts.cluster_uris.0, false).await?;
        let (sink, stream) = NatsTcpStream::new(tcp_stream).await.split();

        let version = 1;
        let client = NatsClient {
            inner: Arc::new(NatsClientInner {
                conn_sink: Arc::new(Mutex::new(sink)),
                opts,
                server_info: RwLock::new(None),
                subscriptions: Arc::new(Mutex::new(HashMap::default())),
                on_reconnect: tokio::sync::Mutex::new(None),
                state: RwLock::new(NatsClientState::Connecting),
                last_ping: RwLock::new(NatsClientInner::time_in_millis()),
                reconnect_version: RwLock::new(version),
                client_ref: RwLock::new(None),
            }),
            reconnect_handlers: RwLock::new(Vec::new()),
        };
        match NatsClientInner::start(client.inner.clone(), version,stream).await {
            Ok(_) => {},
            Err(err) => {
                let _ = client.close().await;
                return Err(err)
            }
        }

        let arc_client = Arc::new(client);
        let reconn_client = arc_client.clone();

        {
            let mut client_ref = arc_client.inner.client_ref.write().await;
            *client_ref = Some(arc_client.clone());
        }

        {
            let mut reconnect = arc_client.inner.on_reconnect.lock().await;
            let reconnect_f = async move {
                reconn_client.on_reconnect().await
            };
            *reconnect = Some(Box::pin(reconnect_f));
        }

        //heartbeat monitor
        let heartbeat_client = arc_client.clone();
        tokio::spawn(async move {
            let _ = heartbeat_client.inner.monitor_heartbeat().await;
        });
        Ok(arc_client)
    }

    pub async fn subscribe<T>(
        &self,
        subject: T,
    ) -> Result<(NatsSid, impl Stream<Item=Message> + Send + Sync), RatsioError>
        where T: ToString {
        let cmd = Subscribe {
            subject: subject.to_string(),
            ..Default::default()
        };
        self.inner.subscribe(cmd).await
    }

    pub async fn subscribe_with_group<T>(
        &self,
        subject: T,
        group: T,
    ) -> Result<(NatsSid, impl Stream<Item=Message> + Send + Sync), RatsioError>
        where T: ToString {
        let cmd = Subscribe {
            subject: subject.to_string(),
            queue_group: Some(group.to_string()),
            ..Default::default()
        };
        self.inner.subscribe(cmd).await
    }

    pub async fn un_subscribe(
        &self,
        sid: &NatsSid,
    ) -> Result<(), RatsioError> {
        self.inner.un_subscribe(sid.clone()).await
    }

    pub async fn publish<T>(
        &self,
        subject: T,
        data: &[u8],
    ) -> Result<(), RatsioError>
        where T: ToString {
        let cmd = Publish {
            subject: subject.to_string(),
            reply_to: None,
            payload: Vec::from(data),
        };
        self.inner.publish(cmd).await
    }


    pub async fn publish_with_reply_to<T>(
        &self,
        subject: T,
        reply_to: T,
        data: &[u8],
    ) -> Result<(), RatsioError>
        where T: ToString {
        let cmd = Publish {
            subject: subject.to_string(),
            reply_to: Some(reply_to.to_string()),
            payload: Vec::from(data),
        };
        self.inner.publish(cmd).await
    }


    pub async fn request<T>(
        &self,
        subject: T,
        data: &[u8],
    ) -> Result<Message, RatsioError>
        where T: ToString {
        let cmd = Publish {
            subject: subject.to_string(),
            payload: Vec::from(data),
            reply_to: None,
        };
        self.inner.request(cmd).await
    }

    pub async fn close(&self) -> Result<(), RatsioError> {
        self.inner.stop().await
    }

    pub async fn add_reconnect_handler(&self, handler: ReconnectHandler) -> Result<(), RatsioError> {
        let mut handlers = self.reconnect_handlers.write().await;
        handlers.push(handler);

        Ok(())
    }

    pub (in crate::nats_client) async fn on_reconnect(&self) -> () {
        let handlers = self.reconnect_handlers.read().await;
        let handlers: &Vec<ReconnectHandler> = handlers.as_ref();
        for handler in handlers {
            handler(self)
        }
    }
}
