use crate::nats_client::{NatsClientOptions, UriVec};

impl From<&str> for NatsClientOptions {
    fn from(uri: &str) -> Self {
        NatsClientOptions{
            cluster_uris: UriVec(vec![uri.to_string()]),
            ..Default::default()
        }
    }
}

impl From<Vec<&str>> for NatsClientOptions {
    fn from(uris: Vec<&str>) -> Self {
        NatsClientOptions{
            cluster_uris: UriVec(uris.iter().map(|uri| uri.to_string()).collect()),
            ..Default::default()
        }
    }
}

impl From<Vec<String>> for NatsClientOptions {
    fn from(uris: Vec<String>) -> Self {
        NatsClientOptions{
            cluster_uris: UriVec(uris.iter().map(|uri| uri.to_string()).collect()),
            ..Default::default()
        }
    }
}


impl From<Vec<&str>> for UriVec {
    fn from(xs: Vec<&str>) -> Self {
        UriVec(xs.into_iter().map(|x| x.into()).collect())
    }
}

impl From<Vec<String>> for UriVec {
    fn from(xs: Vec<String>) -> Self {
        UriVec(xs)
    }
}

impl From<String> for UriVec {
    fn from(x: String) -> Self {
        UriVec(vec![x])
    }
}

impl From<&str> for UriVec {
    fn from(x: &str) -> Self {
        UriVec(vec![x.into()])
    }
}
