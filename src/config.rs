#[derive(Debug, Clone)]
pub struct Config {
    pub root_id: Option<String>,
    pub api_token: Option<String>,
    pub port: u16,
    pub host: String,
    pub bypass: bool,
}
