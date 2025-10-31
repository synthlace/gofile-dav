use std::{collections::HashMap, result::Result as StdResult, str::FromStr};

use super::error::{Error, Result as GofileResult};
use serde::{Deserialize, Deserializer, Serialize, de};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum IdOrCode {
    Uuid4 { uuid: Uuid },
    Code { code: String },
}

impl std::fmt::Display for IdOrCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdOrCode::Uuid4 { uuid } => write!(f, "{}", uuid),
            IdOrCode::Code { code } => write!(f, "{}", code),
        }
    }
}

impl FromStr for IdOrCode {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(Self::Uuid4 { uuid }),
            Err(_) => Ok(Self::Code { code: s.to_owned() }),
        }
    }
}

// TODO?: Consider refactoring all IDs to `IdOrCode`.
// These `From` impls exist to allow passing strings or references into functions
// expecting `Into<IdOrCode>` without refactoring all existing IDs. They make
// the API flexible for both `String`, `&str`, and `&IdOrCode` inputs.

impl From<&IdOrCode> for IdOrCode {
    fn from(s: &IdOrCode) -> Self {
        s.clone()
    }
}

impl From<&str> for IdOrCode {
    fn from(s: &str) -> Self {
        Self::from_str(s).unwrap()
    }
}

impl From<String> for IdOrCode {
    fn from(s: String) -> Self {
        Self::from_str(&s).unwrap()
    }
}

impl From<&String> for IdOrCode {
    fn from(s: &String) -> Self {
        Self::from_str(s).unwrap()
    }
}

/// Top-level response
#[derive(Debug, Serialize)]
pub enum ApiResponse<T> {
    Ok { data: T },
    NotFound,
    RateLimit,
    InvalidToken,
    NotPremium,
    Other { status: String },
}

impl<T> ApiResponse<T> {
    pub fn into_result(self) -> GofileResult<T> {
        match self {
            ApiResponse::Ok { data } => Ok(data),
            ApiResponse::NotFound => Err(Error::NotFound),
            ApiResponse::RateLimit => Err(Error::Api {
                status: "error-rateLimit".into(),
            }),
            ApiResponse::InvalidToken => Err(Error::Api {
                status: "error-token".into(),
            }),
            ApiResponse::NotPremium => Err(Error::Api {
                status: "error-notPremium".into(),
            }),
            ApiResponse::Other { status } => Err(Error::Api { status }),
        }
    }
}

impl<'de, T> Deserialize<'de> for ApiResponse<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;

        let status = map
            .remove("status")
            .ok_or_else(|| de::Error::missing_field("status"))?
            .as_str()
            .ok_or_else(|| de::Error::custom("status must be a string"))?
            .to_string();

        match status.as_str() {
            // TODO?: "success" is from bypass response
            "ok" | "success" => {
                let data_value = map
                    .remove("data")
                    .ok_or_else(|| de::Error::missing_field("data"))?;

                let data = T::deserialize(data_value).map_err(de::Error::custom)?;

                Ok(ApiResponse::Ok { data })
            }

            "error-notFound" => Ok(ApiResponse::NotFound),
            "error-rateLimit" => Ok(ApiResponse::RateLimit),
            "error-token" => Ok(ApiResponse::InvalidToken),
            "error-notPremium" => Ok(ApiResponse::NotPremium),

            _ => Ok(ApiResponse::Other { status }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Contents {
    #[serde(rename = "file")]
    File(FileEntry),

    #[serde(rename = "folder")]
    Folder(FolderEntry),
}

impl Contents {
    pub fn name(&self) -> &str {
        match self {
            Self::Folder(folder) => &folder.name,
            Self::File(file) => &file.name,
        }
    }

    pub fn id(&self) -> &Uuid {
        match self {
            Self::File(file_entry) => &file_entry.id,
            Self::Folder(folder_entry) => &folder_entry.id,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::File(file_entry) => file_entry.size,
            Self::Folder(folder_entry) => folder_entry.total_size,
        }
    }

    pub fn created(&self) -> u64 {
        match self {
            Self::File(file) => file.create_time,
            Self::Folder(folder) => folder.create_time,
        }
    }

    pub fn modtime(&self) -> u64 {
        match self {
            Self::File(file) => file.mod_time,
            Self::Folder(folder) => folder.mod_time,
        }
    }
}

pub type ContentsResponse = ApiResponse<Contents>;

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct AccountId {
//     pub id: String,
// }

// pub type AccountIdResponse = ApiResponse<AccountId>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateGuestAccount {
    pub id: String,
    pub root_folder: Uuid,
    pub tier: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub id: String,
    pub root_folder: Uuid,
    pub tier: String,
    pub token: String,
    pub email: String,
}

pub type CreateGuestAccountResponse = ApiResponse<CreateGuestAccount>;
pub type AccountInfoResponse = ApiResponse<AccountInfo>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub can_access: bool,
    pub id: Uuid,
    pub name: String,
    pub create_time: u64,
    pub mod_time: u64,
    pub size: u64,
    pub md5: String,
    pub link: Url,
    pub download_count: u64,
    pub servers: Vec<String>,
    pub server_selected: String,
    pub parent_folder: String,
    #[serde(default = "_default_false")]
    pub is_owner: bool,
    #[serde(default = "_default_false")]
    pub is_frozen: bool,
    pub mimetype: Option<String>,
    pub thumbnail: Option<String>,
    pub is_frozen_timestamp: Option<u64>,

    #[serde(default = "_default_false")]
    pub bypassed: bool,
}

fn _default_true() -> bool {
    true
}

fn _default_false() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderEntry {
    #[serde(default = "_default_true")]
    pub is_dir: bool,
    pub can_access: bool,
    pub id: Uuid,
    pub name: String,
    pub create_time: u64,
    pub mod_time: u64,
    pub total_size: u64,
    pub code: String,
    pub public: bool,
    pub total_download_count: u64,
    pub children_count: i64,
    pub parent_folder: Option<String>,
    #[serde(default = "_default_false")]
    pub is_owner: bool,
    #[serde(default)]
    pub children: HashMap<String, Contents>,
}

// #[derive(Debug, Serialize, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub struct Metadata {
//     pub total_count: u64,
//     pub total_pages: u64,
//     pub page: u64,
//     pub page_size: u64,
//     pub has_next_page: bool,
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BypassFile {
    pub name: String,
    pub size: u64,
    pub link: Url,
    pub proxy_link: Url,
}

pub type BypassFiles = Vec<BypassFile>;
pub type BypassFilesResponse = ApiResponse<BypassFiles>;

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_ok_api_response() {
        let value = json!({
            "status": "ok",
            "data": {
                "canAccess": true,
                "id": "6c9e22a7-7d6c-4986-8e93-b118558be0bb",
                "type": "folder",
                "name": "root",
                "createTime": 1719990416,
                "modTime": 1719990416,
                "code": "Veil7n",
                "public": false,
                "totalDownloadCount": 0,
                "totalSize": 0,
                "childrenCount": 0,
                "children": {}
            }
        });

        let parsed: ContentsResponse = serde_json::from_value(value).unwrap();
        match parsed {
            ApiResponse::Ok {
                data: Contents::Folder(folder),
            } => {
                assert!(folder.can_access);
                assert_eq!(
                    folder.id,
                    Uuid::from_str("6c9e22a7-7d6c-4986-8e93-b118558be0bb").unwrap()
                );
                assert_eq!(folder.name, "root");
                assert_eq!(folder.create_time, 1719990416);
                assert_eq!(folder.mod_time, 1719990416);
                assert_eq!(folder.code, "Veil7n");
                assert_eq!(folder.public, false);
                assert_eq!(folder.total_download_count, 0);
                assert_eq!(folder.total_size, 0);
                assert_eq!(folder.children_count, 0);
                assert!(folder.children.is_empty());
            }
            other => panic!("expected ApiResponse::Ok::Folder, got {:?}", other),
        }
    }

    #[test]
    fn test_other_api_response() {
        let value = json!({"status":"error-notPremium","data":{}});
        let parsed: ContentsResponse = serde_json::from_value(value).unwrap();

        assert!(
            matches!(parsed, ApiResponse::NotPremium),
            "expected ApiResponse::NotPremium",
        )
    }

    #[test]
    fn test_unexpected_api_response() {
        for input in [r#"{"verde": true}"#, r#""#] {
            let result = serde_json::from_str::<ContentsResponse>(input);
            assert!(result.is_err());
        }
    }
}
