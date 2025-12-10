use std::{collections::HashMap, result::Result as StdResult, time::Duration};

use super::{
    error::{Error, Result},
    model::{
        AccountInfo, AccountInfoResponse, Attribute, BypassFiles, BypassFilesResponse, Contents,
        ContentsOk, ContentsRestricted, ContentsUdpated, ContentsUdpatedResponse,
        ContentsWithPassword, ContentsWithPasswordResponse, CreateFolderPayload,
        CreateGuestAccount, CreateGuestAccountResponse, DeleteContentsPayload,
        DeleteContentsResponse, DeletedContents, FileUploaded, FileUploadedResponse, FolderCreated,
        FolderCreatedResponse, IdOrCode,
    },
};

use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use log::{error, warn};
use reqwest::{
    Client as RqwClient, IntoUrl, Method, RequestBuilder as RqwRequestBuilder,
    header::REFERER,
    multipart::{Form, Part},
};
use reqwest_middleware::{
    ClientBuilder as MiddlewareClientBuilder, ClientWithMiddleware, RequestBuilder,
};
use reqwest_retry::{Jitter, RetryTransientMiddleware, policies::ExponentialBackoff};
use tokio::sync::OnceCell;

const API_BASE_URL: &str = "https://api.gofile.io";
const API_BASE_UPLOAD_URL: &str = "https://upload.gofile.io";
const DEFAULT_MAX_RETRIES: u32 = 10;
const REFERER_HEADER: &str = "https://gofile.io/";
const GOFILE_JS_WT_URL: &str = "https://gofile.io/dist/js/config.js";
// JS Number.MAX_SAFE_INTEGER
const DEFAULT_PAGE_SIZE: &str = "9007199254740991";

const BYPASS_API_URL: &str = "https://gf.1drv.eu.org";
const BYPASS_GAMBLE_MAX_RETRIES: u32 = 10;
const BROKEN_BYPASS_PROXY_URL_HOSTS: &[&str] = &["gf.cybar.xyz"];

static WT_TOKEN: OnceCell<String> = OnceCell::const_new();

pub struct ClientBuilder {
    client: Option<RqwClient>,
    api_token: Option<String>,
    password: Option<String>,
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
            password: None,
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

    pub fn with_password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    pub fn build(self) -> Client {
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_millis(500), Duration::from_secs(20))
            .base(2)
            .jitter(Jitter::Bounded)
            .build_with_max_retries(DEFAULT_MAX_RETRIES);

        let raw_client = self.client.unwrap_or_default();
        let client = MiddlewareClientBuilder::new(raw_client.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let api_token = OnceCell::new_with(self.api_token);

        let password = self.password;

        Client {
            raw_client,
            client,
            api_token,
            password,
            use_bypass: self.bypass,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    raw_client: RqwClient,
    client: ClientWithMiddleware,
    api_token: OnceCell<String>,
    password: Option<String>,
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
                    .get(GOFILE_JS_WT_URL)
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

        let mut params: Vec<(&str, &str)> = Vec::with_capacity(3);
        params.push(("page", "1"));
        params.push(("pageSize", DEFAULT_PAGE_SIZE));
        if let Some(pw) = self.password.as_deref() {
            params.push(("password", pw));
        }

        let result = self
            .auth_request_builder(Method::GET, format!("/contents/{}", content_id))
            .await?
            .header("X-Website-Token", wt_token)
            .query(&params)
            .send()
            .await?
            .json::<ContentsWithPasswordResponse>()
            .await?
            .into_result()?;

        let contents = match result {
            // Nothing to do here - the password has already been applied
            ContentsWithPassword::Restricted(restricted_contents) => {
                return Err(restricted_contents.into_err());
            }
            ContentsWithPassword::Ok(contents) => contents,
        };

        let mut folder_entry_ok = match *contents {
            ContentsOk::File(file_entry) => return Ok(Contents::File(file_entry)),
            ContentsOk::Folder(folder_entry) => folder_entry,
        };

        let children = std::mem::take(&mut folder_entry_ok.children);
        let mut folder_entry = folder_entry_ok.into_folder_entry_empty();

        let mut folders_to_process = vec![];

        let mut new_children = HashMap::new();

        for (uuid, child) in children {
            match child {
                ContentsWithPassword::Ok(contents_ok) => match *contents_ok {
                    ContentsOk::File(file_entry_ok) => {
                        new_children.insert(uuid, Contents::File(file_entry_ok));
                    }
                    ContentsOk::Folder(folder_entry_ok) => {
                        new_children.insert(
                            uuid,
                            Contents::Folder(folder_entry_ok.into_folder_entry_empty()),
                        );
                    }
                },
                ContentsWithPassword::Restricted(contents_restricted) => {
                    match contents_restricted {
                        ContentsRestricted::File(_) => {
                            // Technically impossible. Occurs only as a top-level entry.
                            warn!("hit restricted file {}", uuid);
                            continue;
                        }
                        ContentsRestricted::Folder(folder_entry_restricted) => {
                            folders_to_process.push(folder_entry_restricted.id)
                        }
                    }
                }
            }
        }

        for folder_id in folders_to_process {
            let result = self
                .auth_request_builder(Method::GET, format!("/contents/{}", folder_id))
                .await?
                .query(&params)
                .send()
                .await?
                .json::<ContentsWithPasswordResponse>()
                .await?
                .into_result()?;

            match result {
                ContentsWithPassword::Ok(contents_ok) => match *contents_ok {
                    ContentsOk::File(file_entry_ok) => {
                        return Err(
                            anyhow!("expected folder but got file {}", file_entry_ok.id).into()
                        );
                    }
                    ContentsOk::Folder(folder_entry_ok) => {
                        let folder_entry = folder_entry_ok.into_folder_entry_empty();
                        new_children.insert(folder_id.to_string(), Contents::Folder(folder_entry));
                    }
                },
                ContentsWithPassword::Restricted(contents_restricted) => {
                    error!("expected ok contents but got restricted on {}", folder_id);

                    return Err(contents_restricted.into_err());
                }
            }
        }

        folder_entry.children = new_children;

        Ok(Contents::Folder(folder_entry))
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
            let (mut contents, bypass_files) = {
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

                        if folder_entry.password {
                            warn!(
                                "Bypass cannot be used on folders with password {} - returning regular contents",
                                folder_entry.id
                            );
                            return Ok(contents);
                        }

                        // Bypass doesn't work for folders that don't contain any files
                        if !folder_entry
                            .children
                            .values()
                            .any(|v| matches!(v, Contents::File(_)))
                        {
                            return Ok(contents);
                        }

                        let bypass_files = self.get_bypass_files(&folder_entry.code).await?;

                        (contents, bypass_files)
                    }
                    Contents::File(ref file_entry) => {
                        if file_entry.password {
                            warn!(
                                "Bypass cannot be used on file with password {} - returning regular contents",
                                file_entry.id
                            );
                            return Ok(contents);
                        }

                        let parrent_contents =
                            self.get_contents(file_entry.parent_folder.as_str()).await?;

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
            };

            if let Contents::Folder(ref mut folder_entry) = contents {
                if !folder_entry.public {
                    return Ok(contents);
                }

                for bypass_file in bypass_files {
                    for (id, content) in folder_entry.children.iter_mut() {
                        if let Contents::File(file_entry) = content
                            && (bypass_file.link.as_str().contains(id))
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

    pub async fn request_builder_for_upload(
        &self,
        parrent_id: impl Into<IdOrCode>,
        file_part: Part,
    ) -> Result<RqwRequestBuilder> {
        let parrent_id = parrent_id.into();
        let api_token = self.get_or_create_guest_token().await?;

        let form = Form::new()
            .part("token", Part::text(api_token))
            .part("folderId", Part::text(parrent_id.to_string()))
            .part("file", file_part); // insert directly

        Ok(self
            .raw_client
            .request(Method::POST, format!("{API_BASE_UPLOAD_URL}/uploadfile"))
            .header(REFERER, REFERER_HEADER)
            .multipart(form))
    }

    pub async fn upload_file(
        &self,
        parrent_id: impl Into<IdOrCode>,
        file_part: Part,
    ) -> Result<FileUploaded> {
        self.request_builder_for_upload(parrent_id, file_part)
            .await?
            .send()
            .await?
            .json::<FileUploadedResponse>()
            .await?
            .into_result()
    }

    pub async fn create_folder(
        &self,
        parrent_id: impl Into<IdOrCode>,
        folder_name: impl AsRef<str>,
    ) -> Result<FolderCreated> {
        let parent_id = parrent_id.into().to_string();
        let payload = CreateFolderPayload {
            parent_folder_id: parent_id.as_ref(),
            folder_name: folder_name.as_ref(),
        };

        self.auth_request_builder(Method::POST, "/contents/createfolder")
            .await?
            .json(&payload)
            .send()
            .await?
            .json::<FolderCreatedResponse>()
            .await?
            .into_result()
    }

    pub async fn update_attribute(
        &self,
        content_id: impl Into<IdOrCode>,
        attribute: Attribute<'_>,
    ) -> Result<ContentsUdpated> {
        let content_id = content_id.into();

        self.auth_request_builder(Method::PUT, format!("/contents/{content_id}/update"))
            .await?
            .json(&attribute)
            .send()
            .await?
            .json::<ContentsUdpatedResponse>()
            .await?
            .into_result()
    }

    pub async fn delete_contents<T, U>(&self, content_ids: T) -> Result<DeletedContents>
    where
        T: AsRef<[U]>,
        U: Into<IdOrCode> + Clone,
    {
        let contents_id = content_ids
            .as_ref()
            .iter()
            .cloned()
            .map(|v| v.into().to_string())
            .collect::<Vec<_>>()
            .join(",");

        let payload = DeleteContentsPayload {
            contents_id: &contents_id,
        };

        self.auth_request_builder(Method::DELETE, "/contents")
            .await?
            .json(&payload)
            .send()
            .await?
            .json::<DeleteContentsResponse>()
            .await?
            .into_result()
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
