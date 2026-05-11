use futures_util::stream::{SplitSink, SplitStream};
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::Message},
    MaybeTlsStream, WebSocketStream,
};
use tracing::info;
use url::Url;

const DEFAULT_WSS_URL: &str = "wss://api.openai.com/v1/realtime";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealtimeProtocol {
    OpenAI,
    Azure,
}

pub struct RealtimeClient {
    pub wss_url: String,
    pub api_key: String,
    pub model: String,
    pub protocol: RealtimeProtocol,
}

impl RealtimeClient {
    pub fn new(api_key: String, model: String) -> Self {
        Self::new_with_endpoint(DEFAULT_WSS_URL.to_owned(), api_key, model)
    }

    pub fn new_with_endpoint(wss_url: String, api_key: String, model: String) -> Self {
        Self::new_with_endpoint_and_protocol(wss_url, api_key, model, RealtimeProtocol::OpenAI)
    }

    pub fn new_with_endpoint_and_protocol(
        wss_url: String,
        api_key: String,
        model: String,
        protocol: RealtimeProtocol,
    ) -> Self {
        Self {
            wss_url,
            api_key,
            model,
            protocol,
        }
    }

    pub async fn connect(
        &self,
    ) -> Result<
        (
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let url = if self.model.trim().is_empty() {
            self.wss_url.clone()
        } else {
            format!("{}?model={}", self.wss_url, self.model)
        };
        let connect_url = match self.protocol {
            RealtimeProtocol::OpenAI => url,
            RealtimeProtocol::Azure => self.with_azure_api_key_query(&url)?,
        };

        info!(
            "Realtime websocket URL: {}",
            self.redact_url_for_log(&connect_url)
        );
        let mut request = connect_url.clone().into_client_request()?;
        self.apply_auth_header(request.headers_mut())?;
        // Since  we are live streaming audio, we disable the nagle algorithm.
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(request, None, true).await?;
        let (write, read) = ws_stream.split();
        Ok((write, read))
    }

    fn apply_auth_header(
        &self,
        headers: &mut tokio_tungstenite::tungstenite::http::HeaderMap,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.protocol {
            RealtimeProtocol::OpenAI => {
                headers.insert("Authorization", format!("Bearer {}", self.api_key).parse()?);
            }
            RealtimeProtocol::Azure => {}
        }

        Ok(())
    }

    fn with_azure_api_key_query(
        &self,
        request_url: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        match self.protocol {
            RealtimeProtocol::OpenAI => Ok(request_url.to_string()),
            RealtimeProtocol::Azure => {
                let mut parsed = Url::parse(request_url)?;

                let has_api_key = parsed.query_pairs().any(|(k, _)| k == "api-key");
                if !has_api_key {
                    parsed
                        .query_pairs_mut()
                        .append_pair("api-key", &self.api_key);
                }

                Ok(parsed.to_string())
            }
        }
    }

    fn redact_url_for_log(&self, request_url: &str) -> String {
        let Ok(mut parsed) = Url::parse(request_url) else {
            return request_url.to_string();
        };

        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(k, v)| {
                if k == "api-key" {
                    (k.into_owned(), "***".to_string())
                } else {
                    (k.into_owned(), v.into_owned())
                }
            })
            .collect();

        if !pairs.is_empty() {
            parsed.query_pairs_mut().clear();
            for (k, v) in pairs {
                parsed.query_pairs_mut().append_pair(&k, &v);
            }
        }

        parsed.to_string()
    }
}
