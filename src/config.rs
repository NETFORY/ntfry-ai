// config.rs — config.toml прокси (майнерский стиль: один файл рядом с бинарём).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Публичные AI-трекеры Netfory (тот же пул, что у ai-worker).
pub const DEFAULT_TRACKERS: &[&str] = &[
    "http://159.69.188.60:7374",
    "http://91.99.119.119:7374",
    "http://212.193.6.217:7374",
    "http://146.103.108.25:7374",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP-порт API (0.0.0.0:<port>)
    #[serde(default = "d_port")]
    pub port: u16,
    #[serde(default = "d_trackers")]
    pub trackers: Vec<String>,
    /// max_tokens по умолчанию, если клиент не задал (шлюз клампит 1..2048)
    #[serde(default = "d_max_tokens")]
    pub default_max_tokens: usize,
    /// запросов в минуту на API-ключ (0 = без лимита)
    #[serde(default = "d_rate")]
    pub rate_limit_per_min: u32,
    /// hex 32 байта — постоянный iroh-ключ прокси (генерируется сам)
    #[serde(default)]
    pub iroh_secret: String,
    /// файл ключей (относительно каталога конфига)
    #[serde(default = "d_keys")]
    pub keys_file: String,
    /// TTL кэша списка моделей из трекера, секунды
    #[serde(default = "d_ttl")]
    pub swarm_cache_secs: u64,
}

fn d_port() -> u16 {
    9871
}
fn d_max_tokens() -> usize {
    1024
}
fn d_rate() -> u32 {
    60
}
fn d_keys() -> String {
    "keys.json".into()
}
fn d_ttl() -> u64 {
    10
}
fn d_trackers() -> Vec<String> {
    DEFAULT_TRACKERS.iter().map(|s| s.to_string()).collect()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: d_port(),
            trackers: d_trackers(),
            default_max_tokens: d_max_tokens(),
            rate_limit_per_min: d_rate(),
            iroh_secret: String::new(),
            keys_file: d_keys(),
            swarm_cache_secs: d_ttl(),
        }
    }
}

impl Config {
    pub fn load_or_create(path: &Path) -> Self {
        if let Ok(raw) = std::fs::read_to_string(path) {
            match toml::from_str(&raw) {
                Ok(c) => return c,
                Err(e) => {
                    eprintln!("плохой config.toml ({e}) — использую дефолты");
                    return Self::default();
                }
            }
        }
        let c = Self::default();
        let _ = c.save(path);
        c
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        let raw = toml::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(path, raw).map_err(|e| e.to_string())
    }

    /// Постоянный iroh-ключ: из конфига или сгенерировать и сохранить.
    pub fn iroh_secret_bytes(&mut self, path: &Path) -> [u8; 32] {
        if let Ok(b) = hex::decode(self.iroh_secret.trim()) {
            if b.len() == 32 {
                let mut s = [0u8; 32];
                s.copy_from_slice(&b);
                return s;
            }
        }
        use rand::RngCore;
        let mut s = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut s);
        self.iroh_secret = hex::encode(s);
        let _ = self.save(path);
        s
    }

    pub fn keys_path(&self, config_path: &Path) -> PathBuf {
        let p = Path::new(&self.keys_file);
        if p.is_absolute() {
            return p.to_path_buf();
        }
        config_path.parent().unwrap_or(Path::new(".")).join(p)
    }
}

/// "tracker.host" → "http://tracker.host" (как в ai-worker).
pub fn normalize_tracker(t: &str) -> String {
    let t = t.trim().trim_end_matches('/');
    if t.starts_with("http://") || t.starts_with("https://") {
        t.to_string()
    } else {
        format!("http://{t}")
    }
}
