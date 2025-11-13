use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("an HTTP error occurred")]
    Http {
        #[from]
        source: reqwest_middleware::Error,
    },
    #[error("an I/O error occurred")]
    Io {
        #[from]
        source: std::io::Error,
    },
    #[error("API returned an error: {status}")]
    Api { status: String },
    #[error("failed to parse wt token from JS")]
    ParseTokenFailed,
    #[error("password required")]
    PasswordRequired,
    #[error("password wrong")]
    PasswordWrong,
    #[error("not found")]
    NotFound,
    #[error("forbidden")]
    Forbidden,
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        reqwest_middleware::Error::from(value).into()
    }
}
