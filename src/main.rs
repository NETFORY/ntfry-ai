// ============================================================
//  ntfry-ai — OpenAI/LM Studio-совместимый API-прокси в AI-рой NETFORY.
//
//  Обычный интернет → HTTP :9871 → трекер (список моделей/шлюзов) →
//  iroh QUIC ai:// → шлюз конвейера → стрим токенов → SSE клиенту.
//  Документация: docs/NTFRY-AI.md
// ============================================================

mod api;
mod config;
mod keys;
mod net;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ntfry-ai", version, about = "NETFORY AI Proxy — OpenAI-совместимый API в децентрализованную AI-сеть")]
struct Cli {
    /// путь к config.toml (создаётся сам при первом запуске)
    #[arg(long, global = true, default_value = "config.toml")]
    config: std::path::PathBuf,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Запустить прокси-сервер (команда по умолчанию)
    Serve {
        /// перекрыть порт из конфига
        #[arg(long)]
        port: Option<u16>,
    },
    /// Сгенерировать API-ключ sk-ntfry-… (пишется в keys.json)
    Keygen {
        /// имя владельца ключа (для статистики)
        #[arg(long, default_value = "default")]
        name: String,
    },
    /// Список ключей и их статистика
    Keys,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let mut cfg = config::Config::load_or_create(&cli.config);
    match cli.cmd.unwrap_or(Cmd::Serve { port: None }) {
        Cmd::Serve { port } => {
            if let Some(p) = port {
                cfg.port = p;
            }
            if let Err(e) = api::serve(cfg, cli.config.clone()).await {
                eprintln!("ошибка: {e}");
                std::process::exit(1);
            }
        }
        Cmd::Keygen { name } => {
            let store = keys::KeyStore::load(&cfg.keys_path(&cli.config));
            let k = store.generate(&name);
            store.save();
            println!("{k}");
            println!("ключ «{name}» сохранён: {}", store.path().display());
        }
        Cmd::Keys => {
            let store = keys::KeyStore::load(&cfg.keys_path(&cli.config));
            store.print_table();
        }
    }
}
