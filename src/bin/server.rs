use clap::{Parser, Subcommand};
use skdb::{
    conf::{ConfigManager, GlobalConfig},
    serv::MainServer,
    TOKIO_RUN,
};
use std::sync::Arc;
use tracing::info_span;

#[derive(Parser, Debug)]
/// Skdb server service
struct StartArgs {
    /// Server start using config yaml
    #[clap(subcommand)]
    command: Command,
}

/// 采用配置文件的方式启动skdb server
#[derive(Parser, Debug)]
struct FileConfig {
    #[clap(short = 'p', help = "config file path to start skdb server")]
    path: String,
}

/// 环境变量或者args参数的方式启动skdb server
#[derive(Parser, Debug)]
struct EnvConfig {
    /// 配置listen的grpc port
    #[clap(
        name = "grpc",
        env = "SKDB_GRPC_PORT",
        default_value_t = 9999,
        short = 'g'
    )]
    grpc_port: u32,

    /// 配置服务发现依赖的redis地址
    #[clap(name = "redis", env = "SKDB_REDIS_ADDR", short = 'r')]
    redis_addr: String,

    /// 配置索引所在的index_dir地址
    #[clap(name = "index", env = "SKDB_IDX_DIR", short = 'i')]
    index_dir: String,

    /// 配置启动环境(local, dev, pre, pro 四种可能)
    #[clap(name = "env", env = "SKDB_ENV", short = 'e')]
    env: String,

    /// 日志输出目录
    #[clap(name = "log", env = "SKDB_LOG", default_value = "./", short = 'l')]
    log_path: String,

    /// 启动服务名称配置
    #[clap(
        name = "serv_name",
        env = "SKDB_SERVER_NAME",
        default_value = "skdb_server",
        short = 'n'
    )]
    app_name: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[clap(name = "conf")]
    ConfigStart(FileConfig),

    #[clap(name = "env")]
    EnvStart(EnvConfig),
}

impl Into<GlobalConfig> for EnvConfig {
    fn into(self) -> GlobalConfig {
        GlobalConfig {
            grpc_port: self.grpc_port,
            redis_addr: self.redis_addr,
            index_dir: self.index_dir,
            env: self.env,
            log_path: self.log_path,
            app_name: self.app_name,
            server_ip: local_ip_address::local_ip()
                .expect("Retrieve local IP failed!")
                .to_string(),
        }
    }
}

fn main() {
    let _span = info_span!("main");
    let args = StartArgs::parse();
    println!("Server started begin to start, args:{:?}", args);

    let global_config = match args.command {
        Command::ConfigStart(file_config) => Arc::new(ConfigManager::load(file_config.path.into())),
        Command::EnvStart(env_config) => Arc::new(env_config.into()),
    };

    let (shutdown_sender, _recv) = tokio::sync::broadcast::channel(1);

    let local_ip = local_ip_address::local_ip().expect("Get current ip failed!");
    let mut main_server = MainServer::new(global_config, local_ip.to_string());

    TOKIO_RUN.block_on(async move {
        main_server.block_start(shutdown_sender).await;
    });
}
