use std::{result::Result as StdResult, time::Duration};

use super::{
    error::{Error, Result},
    model::{
        AccountInfo, AccountInfoResponse, BypassFiles, BypassFilesResponse, Contents,
        ContentsResponse, CreateGuestAccount, CreateGuestAccountResponse, IdOrCode,
    },
};

use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use futures_util::try_join;
use log::warn;
use reqwest::{Client as RqwClient, IntoUrl, Method, header::REFERER};
use reqwest_middleware::{
    ClientBuilder as MiddlewareClientBuilder, ClientWithMiddleware, RequestBuilder,
};
use reqwest_retry::{Jitter, RetryTransientMiddleware, policies::ExponentialBackoff};
use tokio::sync::OnceCell;

const API_BASE_URL: &str = "https://api.gofile.io";
const DEFAULT_MAX_RETRIES: u32 = 10;
const REFERER_HEADER: &str = "https://gofile.io/";
const GOFILE_GLOBALJS_WT_URL: &str = "https://gofile.io/dist/js/global.js";
// JS Number.MAX_SAFE_INTEGER
const DEFAULT_PAGE_SIZE: &str = "9007199254740991";

const BYPASS_API_URL: &str = "https://gf.1drv.eu.org";
const BYPASS_GAMBLE_MAX_RETRIES: u32 = 10;
const BROKEN_BYPASS_PROXY_URL_HOSTS: &[&str] = &["gf.cybar.xyz"];

static WT_TOKEN: OnceCell<String> = OnceCell::const_new();

pub struct ClientBuilder {
    client: Option<RqwClient>,
    api_token: Option<String>,
    bypass: bool,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            client: None,
            api_token: None,
            bypass: false,
        }
    }

    #[allow(unused)]
    pub fn with_client(mut self, client: RqwClient) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.api_token = Some(token.into());
        self
    }

    pub fn use_bypass(mut self, bypass: bool) -> Self {
        self.bypass = bypass;
        self
    }

    pub fn build(self) -> Client {
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_millis(500), Duration::from_secs(20))
            .base(2)
            .jitter(Jitter::Bounded)
            .build_with_max_retries(DEFAULT_MAX_RETRIES);

        let client = MiddlewareClientBuilder::new(self.client.unwrap_or_default())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let api_token = OnceCell::new_with(self.api_token);

        Client {
            client,
            api_token,
            use_bypass: self.bypass,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    client: ClientWithMiddleware,
    api_token: OnceCell<String>,
    use_bypass: bool,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Self {
        ClientBuilder::new().build()
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub async fn request_builder_for_download_stream(
        &self,
        method: Method,
        url: impl IntoUrl,
        bypassed: bool,
    ) -> Result<RequestBuilder> {
        let mut builder = self
            .client
            .request(method, url)
            .header(REFERER, REFERER_HEADER);
        if !bypassed {
            let api_token = self.get_or_create_guest_token().await?;
            builder = builder.bearer_auth(api_token)
        }

        Ok(builder)
    }

    async fn auth_request_builder(
        &self,
        method: Method,
        path: impl AsRef<str>,
    ) -> Result<RequestBuilder> {
        let api_token = self.get_or_create_guest_token().await?;

        Ok(self
            .client
            .request(method, format!("{API_BASE_URL}{}", path.as_ref()))
            .header(REFERER, REFERER_HEADER)
            .bearer_auth(api_token))
    }

    pub async fn get_current_account_info(&self) -> Result<AccountInfo> {
        self.auth_request_builder(Method::GET, "/accounts/website")
            .await?
            .send()
            .await?
            .json::<AccountInfoResponse>()
            .await?
            .into_result()
    }

    pub async fn get_wt_token(&self) -> Result<&'static str> {
        WT_TOKEN
            .get_or_try_init(|| async {
                self.client
                    .get(GOFILE_GLOBALJS_WT_URL)
                    .header(REFERER, REFERER_HEADER)
                    .send()
                    .await?
                    .text()
                    .await?
                    .split("appdata.wt = \"")
                    .nth(1)
                    .and_then(|s| s.split('"').next())
                    .map(|s| s.to_string())
                    .ok_or_else(|| Error::ParseTokenFailed)
            })
            .await
            .map(|s| s.as_ref())
    }

    async fn get_contents_inner(&self, content_id: impl Into<IdOrCode>) -> Result<Contents> {
        let wt_token = self.get_wt_token().await?;
        let content_id = content_id.into();

        self.auth_request_builder(Method::GET, format!("/contents/{}", content_id))
            .await?
            .query(&[
                ("wt", wt_token),
                ("page", "1"),
                ("pageSize", DEFAULT_PAGE_SIZE),
            ])
            .send()
            .await?
            .json::<ContentsResponse>()
            .await?
            .into_result()
    }

    #[async_recursion]
    pub async fn get_contents<T>(&self, content_id: T) -> Result<Contents>
    where
        T: Into<IdOrCode> + Send,
    {
        let content_id = content_id.into();

        if !self.use_bypass {
            return self.get_contents_inner(&content_id).await;
        } else {
            let (mut contents, bypass_files) = match content_id {
                IdOrCode::Uuid4 { .. } => {
                    let contents = self.get_contents_inner(&content_id).await?;

                    match contents {
                        Contents::Folder(ref folder_entry) => {
                            if !folder_entry.public {
                                warn!(
                                    "Bypass cannot be used on private folder {} - returning regular contents",
                                    folder_entry.id
                                );
                                return Ok(contents);
                            }

                            let bypass_files = self.get_bypass_files(&folder_entry.code).await?;

                            (contents, bypass_files)
                        }
                        Contents::File(file_entry) => {
                            let parrent_contents =
                                self.get_contents(file_entry.parent_folder).await?;

                            match parrent_contents {
                                Contents::File(file_entry) => {
                                    return Err(anyhow!(
                                        "Expected folder but got file {}",
                                        file_entry.id
                                    )
                                    .into());
                                }
                                Contents::Folder(parent_folder) => {
                                    return Ok(parent_folder
                                        .children
                                        .values()
                                        .find(|el| el.id() == &file_entry.id)
                                        .with_context(|| {
                                            format!(
                                                "Expected file {} to be found in parent folder {}",
                                                file_entry.id, parent_folder.id
                                            )
                                        })?
                                        .clone());
                                }
                            }
                        }
                    }
                }
                IdOrCode::Code { ref code } => {
                    // Code-form IDs are only used for folders. Since the bypass service only accepts
                    // folder IDs in code form, we can fetch both regular contents and bypass files
                    // concurrently for better performance.
                    try_join!(
                        self.get_contents_inner(&content_id),
                        self.get_bypass_files(code)
                    )?
                }
            };

            if let Contents::Folder(ref mut folder_entry) = contents {
                if !folder_entry.public {
                    return Ok(contents);
                }

                for bypass_file in bypass_files {
                    for content in folder_entry.children.values_mut() {
                        if let Contents::File(file_entry) = content
                            && file_entry.name == bypass_file.name
                        {
                            file_entry.bypassed = true;
                            file_entry.link = bypass_file.proxy_link.clone()
                        }
                    }
                }
            }

            Ok(contents)
        }
    }

    pub async fn create_guest_account(&self) -> Result<CreateGuestAccount> {
        self.client
            .request(Method::POST, format!("{API_BASE_URL}/accounts"))
            .header(REFERER, REFERER_HEADER)
            .send()
            .await?
            .json::<CreateGuestAccountResponse>()
            .await?
            .into_result()
    }

    pub async fn get_or_create_guest_token(&self) -> Result<String> {
        self.api_token
            .get_or_try_init(|| async {
                let token = self.create_guest_account().await?.token;
                Ok(token)
            })
            .await
            .cloned()
    }

    pub async fn get_bypass_files(&self, id: impl AsRef<str>) -> Result<BypassFiles> {
        for _ in 0..BYPASS_GAMBLE_MAX_RETRIES {
            let resp = self
                .client
                .get(format!("{BYPASS_API_URL}/api/files"))
                .query(&[("folderId", id.as_ref())])
                .send()
                .await?;

            if resp.status() == 502 {
                return Err(Error::NotFound);
            }

            let data = resp.json::<BypassFilesResponse>().await?.into_result()?;

            let retry = data
                .first()
                .map(|f| {
                    BROKEN_BYPASS_PROXY_URL_HOSTS
                        .iter()
                        .any(|host| f.proxy_link.host_str() == Some(host))
                })
                .unwrap_or(false);

            if !retry {
                return Ok(data);
            }
        }

        Err(anyhow!(
            "Max retries reached while fetching bypass files for folder {}",
            id.as_ref()
        )
        .into())
    }

    #[allow(unused)]
    pub fn get_api_token(&self) -> Option<&str> {
        self.api_token.get().map(|s| s.as_str())
    }

    #[allow(unused)]
    pub async fn execute(
        &self,
        req: reqwest::Request,
    ) -> StdResult<reqwest::Response, reqwest_middleware::Error> {
        self.client.execute(req).await
    }
}
