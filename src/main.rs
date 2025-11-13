use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use env_logger::Env;

use std::{net::TcpListener, sync::Arc};

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
    ls::DavLockSystem,
    memls::MemLs,
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
        .multiple(true)
    ))]
    /// Run webdav server
    Serve {
        /// Gofile API token
        #[arg(long, short = 't', env)]
        api_token: Option<String>,

        /// Root folder ID
        #[arg(env)]
        root_id: Option<String>,

        /// Root password
        #[arg(long, short = 'P', env)]
        password: Option<String>,

        /// Mode
        #[arg(long, short, env, value_enum, default_value_t = Mode::ReadOnly)]
        mode: Mode,

        /// Port for the application
        #[arg(long, short, env, default_value_t = 4914)]
        port: u16,

        /// Host for the application
        #[arg(long, short = 'H', env, default_value = "127.0.0.1")]
        host: String,

        /// Use public service gofile-bypass.cybar.xyz for downloads
        #[arg(long, short, env)]
        bypass: bool,
    },

    /// Upgrade the binary
    Upgrade,
}

#[derive(Clone, Debug, ValueEnum)]
enum Mode {
    ReadOnly,
    ReadWrite,
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
                password,
                mode,
            } => Ok(Config {
                root_id,
                api_token,
                port,
                host,
                bypass,
                password: password.map(sha256::digest),
                write_enabled: matches!(mode, Mode::ReadWrite),
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

    if let Some(password) = config.password.clone() {
        client = client.with_password(password)
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
            Contents::Folder(folder) => {
                if config.write_enabled && !folder.is_owner {
                    bail!("Write can be used only on an owned folder")
                }

                if config.password.is_some() && folder.is_owner {
                    warn!("no password needed for owned folder");
                }

                folder.code
            }
        },
        Err(Error::NotFound) => bail!("Contents not found {}", root_id),
        Err(e) => return Err(e.into()),
    };

    let dircache = Arc::new(RwLock::new(DirCache::new(root_id)));
    let filesystem = DavFs::new_boxed(client, dircache, config.write_enabled);
    let (methods, locksystem) = if config.write_enabled {
        (
            DavMethodSet::WEBDAV_RW,
            MemLs::new() as Box<dyn DavLockSystem>,
        )
    } else {
        (
            DavMethodSet::WEBDAV_RO,
            FakeLs::new() as Box<dyn DavLockSystem>,
        )
    };

    let dav_server = DavConfig::new()
        .methods(methods)
        .filesystem(filesystem)
        .locksystem(locksystem)
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
