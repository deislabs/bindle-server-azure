mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;

use bindle::{
    search,
    server::{server, TlsConfig},
};
use clap::Clap;

use storage::{AzureBlobConfig, CosmosConfig};

const DESCRIPTION: &str = r#"
The Bindle Server

Bindle is a technology for storing and retrieving aggregate applications.
This program runs an HTTP frontend for a Bindle repository, backed by Azure
services.
"#;

#[derive(Clap)]
#[clap(name = "azure-bindle-server", version = clap::crate_version!(), author = "DeisLabs at Microsoft Azure", about = DESCRIPTION)]
struct Opts {
    #[clap(
        short = 'i',
        long = "address",
        env = "BINDLE_IP_ADDRESS_PORT",
        default_value = "127.0.0.1:8080",
        about = "the IP address and port to listen on"
    )]
    address: String,
    #[clap(
        name = "cert_path",
        short = 'c',
        long = "cert-path",
        env = "BINDLE_CERT_PATH",
        requires = "key_path",
        about = "the path to the TLS certificate to use. If set, --key-path must be set as well. If not set, the server will use HTTP"
    )]
    cert_path: Option<PathBuf>,
    #[clap(
        name = "key_path",
        short = 'k',
        long = "key-path",
        env = "BINDLE_KEY_PATH",
        requires = "cert_path",
        about = "the path to the TLS certificate key to use. If set, --cert-path must be set as well. If not set, the server will use HTTP"
    )]
    key_path: Option<PathBuf>,
    #[clap(
        long = "db-account",
        env = "BINDLE_DB_ACCOUNT",
        about = "The Cosmos account name to use"
    )]
    cosmos_account: String,
    #[clap(
        long = "db-name",
        env = "BINDLE_DB_NAME",
        about = "The Cosmos database name to use. This database should have two collections named `labels` and `invoices`"
    )]
    cosmos_db_name: String,
    #[clap(
        long = "db-auth",
        env = "BINDLE_DB_AUTH",
        about = "The authentication token to use for Cosmos. Should be a primary token with create, read, and update access to the `invoices` collection and create and read access to the `labels` collection"
    )]
    cosmos_auth_token: String,
    #[clap(
        long = "storage-account",
        env = "BINDLE_STORAGE_ACCOUNT",
        about = "The Azure Storage account name to use"
    )]
    storage_account: String,
    #[clap(
        long = "storage-auth",
        env = "BINDLE_STORAGE_AUTH",
        about = "The authentication token to use for Azure storage. Should be a SAS token with create, read, and delete access to the `parcels` container"
    )]
    storage_auth_token: String,
}

#[tokio::main(threaded_scheduler)]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    env_logger::init();

    let addr: SocketAddr = opts.address.parse()?;
    let index = search::StrictEngine::default();
    // TODO: swap this out once configured
    let store = storage::AzureStorage::new(
        CosmosConfig {
            account_name: opts.cosmos_account,
            database_name: opts.cosmos_db_name,
            authorization_token: opts.cosmos_auth_token,
        },
        AzureBlobConfig {
            account_name: opts.storage_account,
            sas_token: opts.storage_auth_token,
        },
    )?;

    log::info!("Starting server at {}", addr.to_string(),);

    // Map doesn't work here because we've already moved data out of opts
    let tls = match opts.cert_path {
        None => None,
        Some(p) => Some(TlsConfig {
            cert_path: p,
            key_path: opts
                .key_path
                .expect("--key-path should be set if --cert-path was set"),
        }),
    };
    server(store, index, addr, tls).await
}
