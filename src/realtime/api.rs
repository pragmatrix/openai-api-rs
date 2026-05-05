use futures_util::stream::{SplitSink, SplitStream};
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::Message},
    MaybeTlsStream, WebSocketStream,
};
use tracing::info;
use url::Url;

const WSS_URL: &str = "wss://api.openai.com/v1/realtime";

pub struct RealtimeClient {
    pub wss_url: String,
    pub api_key: String,
    pub model: String,
}

impl RealtimeClient {
    pub fn new(api_key: String, model: String) -> Self {
        let wss_url = std::env::var("WSS_URL").unwrap_or_else(|_| WSS_URL.to_owned());
        Self::new_with_endpoint(wss_url, api_key, model)
    }

    pub fn new_with_endpoint(wss_url: String, api_key: String, model: String) -> Self {
        Self {
            wss_url,
            api_key,
            model,
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
        let connect_url = self.with_azure_api_key_query(&url)?;

        info!(
            "Realtime websocket URL: {}",
            self.redact_url_for_log(&connect_url)
        );
        let mut request = connect_url.clone().into_client_request()?;
        self.apply_auth_header(connect_url.as_str(), request.headers_mut())?;
        request
            .headers_mut()
            .insert("OpenAI-Beta", "realtime=v1".parse()?);
        // Since  we are live streaming audio, we disable the nagle algorithm.
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(request, None, true).await?;
        let (write, read) = ws_stream.split();
        Ok((write, read))
    }

    fn apply_auth_header(
        &self,
        request_url: &str,
        headers: &mut tokio_tungstenite::tungstenite::http::HeaderMap,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let use_authorization = Url::parse(request_url)
            .ok()
            .map(|url| !Self::is_azure_openai_host(&url))
            .unwrap_or(true);

        if use_authorization {
            headers.insert("Authorization", format!("Bearer {}", self.api_key).parse()?);
        }

        Ok(())
    }

    fn with_azure_api_key_query(
        &self,
        request_url: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut parsed = Url::parse(request_url)?;
        if !Self::is_azure_openai_host(&parsed) {
            return Ok(request_url.to_string());
        }

        let has_api_key = parsed.query_pairs().any(|(k, _)| k == "api-key");
        if !has_api_key {
            parsed
                .query_pairs_mut()
                .append_pair("api-key", &self.api_key);
        }

        Ok(parsed.to_string())
    }

    fn is_azure_openai_host(url: &Url) -> bool {
        url.host_str()
            .map(|host| host.ends_with(".openai.azure.com"))
            .unwrap_or(false)
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
