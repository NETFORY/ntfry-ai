// keys.rs — мок API-ключи (sk-ntfry-…) + статистика и rate-limit на ключ.
// Файл keys.json пуст/отсутствует → ОТКРЫТЫЙ режим (без авторизации).
// В будущем сюда встанет биллинг по price_per_mtok из каталога моделей.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyEntry {
    pub key: String,
    pub name: String,
    pub created_at: u64,
    #[serde(default)]
    pub requests: u64,
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub last_used: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct FileFmt {
    #[serde(default)]
    keys: Vec<KeyEntry>,
}

pub enum Check {
    /// файл ключей пуст — открытый режим
    Open,
    Ok(String),
    Missing,
    Invalid,
    RateLimited,
}

struct Inner {
    keys: Vec<KeyEntry>,
    /// ключ → (минута epoch, счётчик) — фиксированное окно rate-limit
    window: HashMap<String, (u64, u32)>,
    dirty: bool,
}

pub struct KeyStore {
    path: PathBuf,
    inner: Mutex<Inner>,
}

impl KeyStore {
    pub fn load(path: &Path) -> Self {
        let keys = std::fs::read_to_string(path)
            .ok()
            .and_then(|raw| serde_json::from_str::<FileFmt>(&raw).ok())
            .map(|f| f.keys)
            .unwrap_or_default();
        Self {
            path: path.to_path_buf(),
            inner: Mutex::new(Inner { keys, window: HashMap::new(), dirty: false }),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_open(&self) -> bool {
        self.inner.lock().unwrap().keys.is_empty()
    }

    pub fn save(&self) {
        let mut inner = self.inner.lock().unwrap();
        let raw = serde_json::to_string_pretty(&FileFmt { keys: inner.keys.clone() })
            .unwrap_or_else(|_| "{\"keys\":[]}".into());
        if std::fs::write(&self.path, raw).is_ok() {
            inner.dirty = false;
        }
    }

    pub fn save_if_dirty(&self) {
        if self.inner.lock().unwrap().dirty {
            self.save();
        }
    }

    pub fn generate(&self, name: &str) -> String {
        use rand::RngCore;
        let mut b = [0u8; 24];
        rand::rngs::OsRng.fill_bytes(&mut b);
        let key = format!("sk-ntfry-{}", hex::encode(b));
        let mut inner = self.inner.lock().unwrap();
        inner.keys.push(KeyEntry {
            key: key.clone(),
            name: name.to_string(),
            created_at: now(),
            requests: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            last_used: 0,
        });
        inner.dirty = true;
        key
    }

    pub fn check(&self, bearer: Option<&str>, limit_per_min: u32) -> Check {
        let mut inner = self.inner.lock().unwrap();
        if inner.keys.is_empty() {
            return Check::Open;
        }
        let Some(k) = bearer else { return Check::Missing };
        if !inner.keys.iter().any(|e| e.key == k) {
            return Check::Invalid;
        }
        if limit_per_min > 0 {
            let min = now() / 60;
            let e = inner.window.entry(k.to_string()).or_insert((min, 0));
            if e.0 != min {
                *e = (min, 0);
            }
            e.1 += 1;
            if e.1 > limit_per_min {
                return Check::RateLimited;
            }
        }
        Check::Ok(k.to_string())
    }

    pub fn note_usage(&self, key: &str, prompt_tokens: u64, completion_tokens: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = inner.keys.iter_mut().find(|e| e.key == key) {
            e.requests += 1;
            e.prompt_tokens += prompt_tokens;
            e.completion_tokens += completion_tokens;
            e.last_used = now();
            inner.dirty = true;
        }
    }

    fn mask(k: &str) -> String {
        if k.len() > 14 {
            format!("{}…{}", &k[..12], &k[k.len() - 4..])
        } else {
            k.to_string()
        }
    }

    /// Статистика для /v1/netfory/stats (ключи замаскированы).
    pub fn stats_json(&self) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        serde_json::json!({
            "openMode": inner.keys.is_empty(),
            "keys": inner.keys.iter().map(|e| serde_json::json!({
                "key": Self::mask(&e.key),
                "name": e.name,
                "requests": e.requests,
                "promptTokens": e.prompt_tokens,
                "completionTokens": e.completion_tokens,
                "lastUsed": e.last_used,
            })).collect::<Vec<_>>(),
        })
    }

    pub fn print_table(&self) {
        let inner = self.inner.lock().unwrap();
        if inner.keys.is_empty() {
            println!("ключей нет — сервер в ОТКРЫТОМ режиме. Создайте: ntfry-ai keygen --name dev");
            return;
        }
        println!("{:<44} {:<12} {:>8} {:>10} {:>12}", "KEY", "NAME", "REQS", "PROMPT_T", "COMPLETION_T");
        for e in &inner.keys {
            println!(
                "{:<44} {:<12} {:>8} {:>10} {:>12}",
                Self::mask(&e.key),
                e.name,
                e.requests,
                e.prompt_tokens,
                e.completion_tokens
            );
        }
    }
}
