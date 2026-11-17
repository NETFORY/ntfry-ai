// net.rs — мост в рой: iroh QUIC клиент (ALPN netfory/ai/1, тот же протокол,
// что ai-worker/p2p.rs) + опрос трекера (/v1/ai/stats) для списка моделей
// и кандидатов-шлюзов.

use crate::config::normalize_tracker;
use iroh::endpoint::{presets, Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayConfig, RelayUrl, TransportAddr};
use iroh_relay::RelayQuicConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub const ALPN: &[u8] = b"netfory/ai/1";
pub const RELAYS_N1: &[&str] = &["https://relay-fsn7.sth.cx", "https://relay-ru1.sth.cx"];
const MAX_FRAME: usize = 64 * 1024 * 1024;
const FRAME_JSON: u8 = 0;

// ---------- протокол (подмножество ai-worker/p2p.rs, формат идентичен) ----------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Msg {
    Ping,
    Pong { node: String, models: Vec<String>, version: String },
    Infer {
        prompt: String,
        max_tokens: usize,
        #[serde(default)]
        model: Option<String>,
        #[serde(default = "d_true")]
        think: bool,
    },
    Token { t: String },
    Done { node: String, ms: u64, text: String },
    Error { e: String },
    /// multi-turn инференс: рендер каноничным шаблоном чата на шлюзе (v0.13+)
    InferChat {
        messages: Vec<ChatTurn>,
        max_tokens: usize,
        #[serde(default)]
        model: Option<String>,
        #[serde(default = "d_true")]
        think: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatTurn {
    pub role: String,
    pub content: String,
}

fn d_true() -> bool {
    true
}

pub async fn write_msg(w: &mut SendStream, m: &Msg) -> Result<(), String> {
    let data = serde_json::to_vec(m).map_err(|e| e.to_string())?;
    let mut buf = Vec::with_capacity(5 + data.len());
    buf.push(FRAME_JSON);
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&data);
    w.write_all(&buf).await.map_err(|e| e.to_string())
}

pub async fn read_msg(r: &mut RecvStream) -> Result<Msg, String> {
    let mut h = [0u8; 5];
    r.read_exact(&mut h).await.map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes([h[1], h[2], h[3], h[4]]) as usize;
    if h[0] != FRAME_JSON || len > MAX_FRAME {
        return Err("неожиданный кадр".into());
    }
    let mut p = vec![0u8; len];
    r.read_exact(&mut p).await.map_err(|e| e.to_string())?;
    serde_json::from_slice(&p).map_err(|e| e.to_string())
}

// ---------- iroh endpoint ----------

pub async fn bind(secret: [u8; 32]) -> Result<Endpoint, String> {
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .secret_key(iroh::SecretKey::from_bytes(&secret))
        .bind()
        .await
        .map_err(|e| format!("iroh bind: {e}"))?;
    for u in RELAYS_N1 {
        match u.parse::<RelayUrl>() {
            Ok(url) => {
                let rc = RelayConfig::new(url.clone(), Some(RelayQuicConfig::default()));
                endpoint.insert_relay(url, Arc::new(rc)).await;
            }
            Err(e) => warn!("пропущен кривой relay {u}: {e}"),
        }
    }
    Ok(endpoint)
}

pub async fn connect(ep: &Endpoint, id: EndpointId) -> Result<Connection, String> {
    let addr = EndpointAddr::from_parts(id, Vec::<TransportAddr>::new());
    tokio::time::timeout(Duration::from_secs(30), ep.connect(addr, ALPN))
        .await
        .map_err(|_| "таймаут соединения (30с)".to_string())?
        .map_err(|e| format!("connect: {e}"))
}

// ---------- события инференса ----------

#[derive(Debug)]
pub enum Ev {
    Token(String),
    Done { node: String, ms: u64, text: String },
    Error(String),
}

/// Насос ответа шлюза: держит соединение живым, льёт события в канал.
pub async fn pump(
    conn: Connection,
    send: SendStream,
    mut recv: RecvStream,
    tx: tokio::sync::mpsc::Sender<Ev>,
) {
    let _keep = (conn, send); // не ронять QUIC-стрим до конца генерации
    loop {
        match read_msg(&mut recv).await {
            Ok(Msg::Token { t }) => {
                if tx.send(Ev::Token(t)).await.is_err() {
                    break;
                }
            }
            Ok(Msg::Done { node, ms, text }) => {
                let _ = tx.send(Ev::Done { node, ms, text }).await;
                break;
            }
            Ok(Msg::Error { e }) => {
                let _ = tx.send(Ev::Error(e)).await;
                break;
            }
            Ok(_) => {}
            Err(e) => {
                let _ = tx.send(Ev::Error(format!("stream: {e}"))).await;
                break;
            }
        }
    }
}

// ---------- каталог роя из трекера ----------

#[derive(Debug, Clone)]
pub struct SwarmModel {
    pub alias: String,
    /// нод сети с моделью (шарды + полные)
    pub nodes: u64,
    /// живой tok/s по шлюзам (0 = простаивает)
    pub tok_per_sec: f64,
    /// мин. цена, STH за 1 млн токенов (None = бесплатно)
    pub price_per_mtok: Option<u64>,
    /// кандидаты-шлюзы (iroh id), отсортированы по свободному VRAM
    pub gateways: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Swarm {
    pub models: Vec<SwarmModel>,
    pub updated_at: u64,
}

/// Опрос трекеров (блокирующий — звать через spawn_blocking).
pub fn fetch_swarm_blocking(trackers: &[String]) -> Result<Swarm, String> {
    let mut last_err = "трекеры недоступны".to_string();
    for t in trackers {
        let url = format!("{}/v1/ai/stats", normalize_tracker(t));
        let resp = match ureq::get(&url).timeout(Duration::from_secs(8)).call() {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("{t}: {e}");
                continue;
            }
        };
        let Ok(txt) = resp.into_string() else {
            last_err = format!("{t}: битый ответ");
            continue;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else {
            last_err = format!("{t}: битый json");
            continue;
        };
        return Ok(parse_swarm(&v));
    }
    Err(last_err)
}

fn parse_swarm(v: &serde_json::Value) -> Swarm {
    // кандидаты-шлюзы: воркеры без shard-ready/throttled и без '@'-диапазонов
    // (такие ноды принимают Msg::Infer — шлюзы конвейеров и полные исполнители)
    let mut gws: HashMap<String, Vec<(String, u64)>> = HashMap::new();
    for n in v["nodes"].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
        if !n["worker"].as_bool().unwrap_or(false)
            || n["shardReady"].as_bool().unwrap_or(false)
            || n["throttled"].as_bool().unwrap_or(false)
        {
            continue;
        }
        let Some(iroh_id) = n["irohNodeId"].as_str().filter(|s| !s.is_empty()) else {
            continue; // трекер < v1.6 не отдаёт irohNodeId — будет fallback на /pick
        };
        let models: Vec<&str> = n["models"]
            .as_array()
            .map(|a| a.iter().filter_map(|m| m.as_str()).collect())
            .unwrap_or_default();
        if models.iter().any(|m| m.contains('@')) {
            continue; // шард-нода — Infer не принимает
        }
        let vram = n["vramFreeMb"].as_u64().unwrap_or(0);
        for m in models {
            gws.entry(m.to_string()).or_default().push((iroh_id.to_string(), vram));
        }
    }
    // маркетплейс-метрики по моделям
    let mut pinfo: HashMap<String, (f64, Option<u64>)> = HashMap::new();
    for p in v["pipelines"].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
        if let Some(m) = p["model"].as_str() {
            pinfo.insert(
                m.to_string(),
                (p["tokPerSec"].as_f64().unwrap_or(0.0), p["pricePerMtok"].as_u64()),
            );
        }
    }
    let mut models = Vec::new();
    if let Some(map) = v["models"].as_object() {
        for (alias, cnt) in map {
            let mut cand = gws.remove(alias).unwrap_or_default();
            cand.sort_by(|a, b| b.1.cmp(&a.1));
            let (tps, price) = pinfo.get(alias).copied().unwrap_or((0.0, None));
            models.push(SwarmModel {
                alias: alias.clone(),
                nodes: cnt.as_u64().unwrap_or(0),
                tok_per_sec: tps,
                price_per_mtok: price,
                gateways: cand.into_iter().map(|(id, _)| id).collect(),
            });
        }
    }
    // вызываемые модели первыми, затем по числу нод
    models.sort_by(|a, b| {
        (b.gateways.len(), b.nodes).cmp(&(a.gateways.len(), a.nodes))
    });
    Swarm { models, updated_at: v["updatedAt"].as_u64().unwrap_or(0) }
}

/// Fallback для старых трекеров: /v1/ai/pick?model= → один кандидат.
pub fn pick_gateway_blocking(trackers: &[String], model: &str) -> Option<String> {
    for t in trackers {
        let url = format!("{}/v1/ai/pick?model={model}", normalize_tracker(t));
        let Ok(resp) = ureq::get(&url).timeout(Duration::from_secs(8)).call() else {
            continue;
        };
        let Ok(txt) = resp.into_string() else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else { continue };
        if let Some(id) = v["irohNodeId"].as_str().filter(|s| !s.is_empty()) {
            return Some(id.to_string());
        }
    }
    None
}
