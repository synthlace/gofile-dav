use clap::{ArgGroup, Parser, Subcommand};
use env_logger::Env;

use std::net::TcpListener;
use std::sync::Arc;

mod config;
mod gofile;
mod upgrade;
use anyhow::bail;
use config::Config;

use actix_web::{App, HttpServer, middleware::Logger, web};
use dav_server::{
    DavConfig, DavHandler, DavMethodSet,
    actix::{DavRequest, DavResponse},
    fakels::FakeLs,
};
use gofile::{Client, DavFs, DirCache, error::Error, model::Contents};
use log::{info, warn};
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(group(
        ArgGroup::new("auth")
        .args(&["root_id", "api_token"])
        .required(true)
    ))]
    /// Run webdav server
    Serve {
        /// Gofile API token
        #[arg(long, short = 't', env)]
        api_token: Option<String>,

        /// Root folder ID
        #[arg(env)]
        root_id: Option<String>,

        /// Port for the application
        #[arg(long, short, env, default_value_t = 4914)]
        port: u16,

        /// Host for the application
        #[arg(long, env, default_value = "127.0.0.1")]
        host: String,

        /// Use public service gofile-bypass.cybar.xyz for downloads
        #[arg(long, short, env)]
        bypass: bool,
    },

    /// Upgrade the binary
    Upgrade,
}

impl TryFrom<Command> for Config {
    type Error = &'static str;

    fn try_from(cmd: Command) -> Result<Self, Self::Error> {
        match cmd {
            Command::Serve {
                api_token,
                root_id,
                port,
                host,
                bypass,
            } => Ok(Config {
                root_id,
                api_token,
                port,
                host,
                bypass,
            }),
            Command::Upgrade => Err("Cannot create Config from Upgrade command"),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Command::Upgrade = cli.command {
        return upgrade::self_upgrade();
    }

    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let config = Config::try_from(cli.command)?;
    run(config)?;

    Ok(())
}

#[actix_web::main(gofile_dav)]
async fn run(config: Config) -> anyhow::Result<()> {
    let mut client = Client::builder();
    if config.bypass {
        warn!("Running with experimental bypass mode enabled");
        client = client.use_bypass(config.bypass)
    }

    if let Some(api_token) = config.api_token {
        client = client.with_token(api_token)
    }
    let client = client.build();

    let account = client.get_current_account_info().await?;
    info!("Current account: {}", account.email);

    let root_id = if let Some(root_id) = config.root_id {
        root_id
    } else {
        client
            .get_current_account_info()
            .await?
            .root_folder
            .to_string()
    };

    let root_id = match client.get_contents(&root_id).await {
        Ok(contents) => match contents {
            Contents::File(file) => bail!("Expected folder but got file {}", file.id),
            Contents::Folder(folder) => folder.code,
        },
        Err(Error::NotFound) => bail!("Contents not found {}", root_id),
        Err(e) => return Err(e.into()),
    };

    let dircache = Arc::new(RwLock::new(DirCache::new(root_id)));
    let dav_server = DavConfig::new()
        .methods(DavMethodSet::WEBDAV_RO)
        .filesystem(DavFs::new_boxed(client, dircache))
        .locksystem(FakeLs::new())
        .build_handler();

    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&bind_addr)?;

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default().log_target("gofile_dav::server"))
            .app_data(web::Data::new(dav_server.clone()))
            .service(web::resource("/{tail:.*}").to(dav_handler))
    })
    .listen(listener)?
    .run()
    .await?;

    Ok(())
}

async fn dav_handler(req: DavRequest, davhandler: web::Data<DavHandler>) -> DavResponse {
    if let Some(prefix) = req.prefix() {
        let config = DavConfig::new().strip_prefix(prefix);
        davhandler.handle_with(config, req.request).await.into()
    } else {
        davhandler.handle(req.request).await.into()
    }
}
