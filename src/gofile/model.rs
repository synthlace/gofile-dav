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

impl From<Uuid> for IdOrCode {
    fn from(uuid: Uuid) -> Self {
        IdOrCode::Uuid4 { uuid }
    }
}

impl From<&Uuid> for IdOrCode {
    fn from(uuid: &Uuid) -> Self {
        IdOrCode::Uuid4 { uuid: *uuid }
    }
}

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
pub enum ApiResponse<T = serde_json::Value> {
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
    pub fn is_dir(&self) -> bool {
        matches!(self, Self::Folder(_))
    }

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
    pub password: bool,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUploaded {
    pub create_time: u64,
    pub download_page: String,
    pub id: Uuid,
    pub md5: String,
    pub mimetype: String,
    pub mod_time: u64,
    pub name: String,
    pub parent_folder: String,
    pub parent_folder_code: String,
    pub servers: Vec<String>,
    pub size: u64,
    pub r#type: String,
}

pub type FileUploadedResponse = ApiResponse<FileUploaded>;

fn _default_true() -> bool {
    true
}

fn _default_false() -> bool {
    false
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PasswordStatus {
    // No "PasswordOk" variant required — password state is encoded by the
    // response variants (Ok vs Restricted) in ContentsWithPassword.
    // PasswordOk,
    PasswordRequired,
    PasswordWrong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderEntryRestricted {
    pub password_status: PasswordStatus,
    pub can_access: bool,
    pub id: Uuid,
    pub name: String,
    pub create_time: u64,
    pub mod_time: u64,

    #[serde(default = "_default_true")]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntryRestricted {
    pub password_status: PasswordStatus,
    pub can_access: bool,

    #[serde(default = "_default_true")]
    pub public: bool,
}

#[derive(Debug, Clone)]
pub enum ContentsRestricted {
    File(FileEntryRestricted),
    Folder(FolderEntryRestricted),
}

impl ContentsRestricted {
    pub fn into_err(self) -> Error {
        let password_status = match self {
            ContentsRestricted::File(file) => file.password_status,
            ContentsRestricted::Folder(folder) => folder.password_status,
        };

        match password_status {
            PasswordStatus::PasswordRequired => Error::PasswordRequired,
            PasswordStatus::PasswordWrong => Error::PasswordWrong,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ContentsOk {
    #[serde(rename = "file")]
    File(FileEntry),

    #[serde(rename = "folder")]
    Folder(FolderEntryOk),
}

impl<'de> Deserialize<'de> for ContentsRestricted {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value.get("type").and_then(serde_json::Value::as_str) {
            Some("folder") => {
                let folder =
                    FolderEntryRestricted::deserialize(&value).map_err(serde::de::Error::custom)?;
                Ok(ContentsRestricted::Folder(folder))
            }
            _ => {
                let file =
                    FileEntryRestricted::deserialize(&value).map_err(serde::de::Error::custom)?;
                Ok(ContentsRestricted::File(file))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ContentsWithPassword {
    Ok(Box<ContentsOk>),
    Restricted(ContentsRestricted),
}

pub type ContentsWithPasswordResponse = ApiResponse<ContentsWithPassword>;

impl<'de> Deserialize<'de> for ContentsWithPassword {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer).map_err(de::Error::custom)?;

        let status = value.get("passwordStatus").and_then(|s| s.as_str());

        match status {
            Some("passwordRequired") | Some("passwordWrong") => {
                let restricted: ContentsRestricted =
                    serde_json::from_value(value).map_err(de::Error::custom)?;
                Ok(ContentsWithPassword::Restricted(restricted))
            }
            _ => {
                let ok: ContentsOk = serde_json::from_value(value).map_err(de::Error::custom)?;
                Ok(ContentsWithPassword::Ok(ok.into()))
            }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderEntry {
    // #[serde(default = "_default_true")]
    // pub is_dir: bool,
    // pub total_download_count: u64,
    // pub children_count: i64,
    pub can_access: bool,
    pub id: Uuid,
    pub name: String,
    pub create_time: u64,
    pub mod_time: u64,
    #[serde(default = "_default_false")]
    pub password: bool,

    pub total_size: u64,
    pub code: String,
    pub public: bool,
    pub parent_folder: Option<String>,
    #[serde(default = "_default_false")]
    pub is_owner: bool,
    #[serde(default)]
    pub children: HashMap<String, Contents>,
}

/// Same as above, but uses the other child type.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderEntryOk {
    // #[serde(default = "_default_true")]
    // pub is_dir: bool,
    // pub total_download_count: u64,
    // pub children_count: i64,
    pub can_access: bool,
    pub id: Uuid,
    pub name: String,
    pub create_time: u64,
    pub mod_time: u64,
    #[serde(default = "_default_false")]
    pub password: bool,

    pub total_size: u64,
    pub code: String,
    pub public: bool,
    pub parent_folder: Option<String>,
    #[serde(default = "_default_false")]
    pub is_owner: bool,
    #[serde(default)]
    pub children: HashMap<String, ContentsWithPassword>,
}

impl FolderEntryOk {
    // Not suitable for Into<> semantics because it adds empty children
    pub fn into_folder_entry_empty(self) -> FolderEntry {
        let Self {
            can_access,
            id,
            name,
            create_time,
            mod_time,
            total_size,
            code,
            public,
            parent_folder,
            is_owner,
            password,
            ..
        } = self;

        FolderEntry {
            can_access,
            id,
            name,
            create_time,
            mod_time,
            total_size,
            code,
            public,
            parent_folder,
            is_owner,
            password,
            ..FolderEntry::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateFolderPayload<'a> {
    pub folder_name: &'a str,
    pub parent_folder_id: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderCreated {
    pub code: String,
    pub create_time: u64,
    pub id: Uuid,
    pub mod_time: u64,
    pub name: String,
    pub owner: Uuid,
    pub parent_folder: Uuid,
    pub r#type: String,
}

pub type FolderCreatedResponse = ApiResponse<FolderCreated>;

#[derive(Debug, Clone)]
pub enum Attribute<'a> {
    Name(&'a str),
}

impl<'a> Serialize for Attribute<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        match self {
            Self::Name(v) => {
                map.serialize_entry("attribute", "name")?;
                map.serialize_entry("attributeValue", v)?;
            }
        }
        map.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUpdated {
    pub create_time: u64,
    pub id: Uuid,
    pub md5: String,
    pub mimetype: String,
    pub mod_time: u64,
    pub name: String,
    pub parent_folder: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderUpdated {
    pub create_time: u64,
    pub id: Uuid,
    pub mod_time: u64,
    pub name: String,
    pub parent_folder: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentsUdpated {
    #[serde(rename = "file")]
    File(FileUpdated),

    #[serde(rename = "folder")]
    Folder(FolderUpdated),
}

pub type ContentsUdpatedResponse = ApiResponse<ContentsUdpated>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteContentsPayload<'a> {
    /// Comma-separated list of content IDs to delete.
    pub contents_id: &'a str,
}

pub type DeletedContents = HashMap<String, ApiResponse>;
pub type DeleteContentsResponse = ApiResponse<DeletedContents>;

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
    use rstest::*;

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

        let parsed = serde_json::from_value::<ContentsWithPasswordResponse>(value)
            .unwrap()
            .into_result()
            .unwrap();

        let folder_ok = if let ContentsWithPassword::Ok(contents) = parsed {
            if let ContentsOk::Folder(folder) = *contents {
                folder
            } else {
                panic!("expected ContentsWithPassword::ContentsOk::Folder");
            }
        } else {
            panic!("expected ContentsWithPassword::ContentsOk::Folder");
        };

        assert!(folder_ok.can_access);
        assert_eq!(
            folder_ok.id,
            Uuid::from_str("6c9e22a7-7d6c-4986-8e93-b118558be0bb").unwrap()
        );
        assert_eq!(folder_ok.name, "root");
        assert_eq!(folder_ok.create_time, 1719990416);
        assert_eq!(folder_ok.mod_time, 1719990416);
        assert_eq!(folder_ok.code, "Veil7n");
        assert_eq!(folder_ok.public, false);
        assert_eq!(folder_ok.total_size, 0);
        assert!(folder_ok.children.is_empty());
        // assert_eq!(folder.children_count, 0);
        // assert_eq!(folder.total_download_count, 0);
    }

    #[test]
    fn test_other_api_response() {
        let value = json!({"status":"error-notPremium","data":{}});
        let parsed = serde_json::from_value::<ContentsWithPasswordResponse>(value).unwrap();

        assert!(
            matches!(parsed, ApiResponse::NotPremium),
            "expected ApiResponse::NotPremium",
        )
    }

    #[test]
    fn test_unexpected_api_response() {
        for input in [r#"{"verde": true}"#, r#""#] {
            let result = serde_json::from_str::<ContentsWithPasswordResponse>(input);
            assert!(result.is_err());
        }
    }

    #[rstest]
    #[case("passwordRequired", PasswordStatus::PasswordRequired)]
    #[case("passwordWrong", PasswordStatus::PasswordWrong)]
    fn test_folder_with_password_required(#[case] input: &str, #[case] expected: PasswordStatus) {
        let value = json!({
            "status": "ok",
            "data": {
                "canAccess": false,
                "password": true,
                "passwordStatus": input,
                "id": "a02b79ff-ae05-4c73-9861-81be0224e65b",
                "type": "folder",
                "name": "TestFolder",
                "createTime": 1762184779,
                "modTime": 1762186199
            },
            "metadata": {}
        });
        let parsed = serde_json::from_value::<ContentsWithPasswordResponse>(value)
            .unwrap()
            .into_result()
            .unwrap();
        let restricted_folder =
            if let ContentsWithPassword::Restricted(ContentsRestricted::Folder(folder)) = parsed {
                folder
            } else {
                panic!("expected ContentsWithPassword::ContentsRestricted::Folder");
            };

        assert_eq!(restricted_folder.password_status, expected);
        assert_eq!(
            restricted_folder.id,
            Uuid::from_str("a02b79ff-ae05-4c73-9861-81be0224e65b").unwrap()
        );
        assert_eq!(restricted_folder.can_access, false);
        assert_eq!(restricted_folder.name, "TestFolder");
        assert_eq!(restricted_folder.create_time, 1762184779);
        assert_eq!(restricted_folder.mod_time, 1762186199);
    }

    #[rstest]
    #[case("passwordRequired", PasswordStatus::PasswordRequired)]
    #[case("passwordWrong", PasswordStatus::PasswordWrong)]
    fn test_file_with_password_required(#[case] input: &str, #[case] expected: PasswordStatus) {
        let value = json!({
            "status": "ok",
            "data": {
                "canAccess": false,
                "password": true,
                "passwordStatus": input
            },
            "metadata": {}
        });
        let parsed = serde_json::from_value::<ContentsWithPasswordResponse>(value)
            .unwrap()
            .into_result()
            .unwrap();
        let restricted_file =
            if let ContentsWithPassword::Restricted(ContentsRestricted::File(file)) = parsed {
                file
            } else {
                panic!("expected ContentsWithPassword::ContentsRestricted::File");
            };

        assert_eq!(restricted_file.password_status, expected);
        assert_eq!(restricted_file.can_access, false);
    }
}
