// api.rs — OpenAI/LM Studio-совместимые ручки поверх роя.
// GET /v1/models · POST /v1/chat/completions (stream/non-stream) ·
// POST /v1/completions · GET /health · GET /v1/netfory/stats
// Зеркала LM Studio: /api/v1/models, /api/v1/chat, /api/v1/chat/completions

use crate::config::Config;
use crate::keys::{Check, KeyStore};
use crate::net::{self, Ev, Msg};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use iroh::EndpointId;
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

/// таймаут первого токена (prefill длинного промпта на CPU долгий)
const FIRST_TOKEN_SECS: u64 = 600;
/// таймаут тишины между токенами
const IDLE_SECS: u64 = 300;
/// сколько шлюзов пробуем при недоступности (failover)
const FAILOVER_TRIES: usize = 3;

#[derive(Clone)]
struct App {
    cfg: Arc<Config>,
    keys: Arc<KeyStore>,
    ep: Arc<iroh::Endpoint>,
    swarm: Arc<Mutex<Option<(Instant, net::Swarm)>>>,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub async fn serve(mut cfg: Config, cfg_path: PathBuf) -> Result<(), String> {
    let secret = cfg.iroh_secret_bytes(&cfg_path);
    let ep = net::bind(secret).await?;
    info!("iroh поднят: ai://{}", ep.id());
    let keys = Arc::new(KeyStore::load(&cfg.keys_path(&cfg_path)));
    if keys.is_open() {
        warn!("keys.json пуст — ОТКРЫТЫЙ режим (без авторизации). Создайте ключ: ntfry-ai keygen --name dev");
    }
    let port = cfg.port;
    let app = App {
        cfg: Arc::new(cfg),
        keys: keys.clone(),
        ep: Arc::new(ep),
        swarm: Arc::new(Mutex::new(None)),
    };
    // периодическое сохранение статистики ключей
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            keys.save_if_dirty();
        }
    });
    let router = Router::new()
        .route("/v1/models", get(models))
        .route("/api/v1/models", get(models))
        .route("/v1/chat/completions", post(chat))
        .route("/api/v1/chat", post(chat))
        .route("/api/v1/chat/completions", post(chat))
        .route("/v1/completions", post(completions))
        .route("/health", get(health))
        .route("/v1/netfory/stats", get(nstats))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(app);
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| e.to_string())?;
    info!("ntfry-ai слушает http://{addr} — OpenAI-совместимый API AI-роя NETFORY");
    axum::serve(listener, router).await.map_err(|e| e.to_string())
}

// ---------- служебное ----------

fn err_json(status: StatusCode, kind: &str, msg: &str) -> Response {
    (status, Json(json!({"error": {"message": msg, "type": kind, "code": null}}))).into_response()
}

fn auth(app: &App, headers: &HeaderMap) -> Result<Option<String>, Response> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.trim());
    match app.keys.check(bearer, app.cfg.rate_limit_per_min) {
        Check::Open => Ok(None),
        Check::Ok(k) => Ok(Some(k)),
        Check::Missing => Err(err_json(
            StatusCode::UNAUTHORIZED,
            "invalid_request_error",
            "нет API-ключа: добавьте заголовок Authorization: Bearer sk-ntfry-…",
        )),
        Check::Invalid => Err(err_json(StatusCode::UNAUTHORIZED, "invalid_request_error", "неверный API-ключ")),
        Check::RateLimited => Err(err_json(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "превышен лимит запросов в минуту для этого ключа",
        )),
    }
}

async fn swarm_get(app: &App) -> Result<net::Swarm, String> {
    if let Some((t, s)) = app.swarm.lock().unwrap().clone() {
        if t.elapsed().as_secs() < app.cfg.swarm_cache_secs {
            return Ok(s);
        }
    }
    let trackers = app.cfg.trackers.clone();
    let s = tokio::task::spawn_blocking(move || net::fetch_swarm_blocking(&trackers))
        .await
        .map_err(|e| e.to_string())??;
    *app.swarm.lock().unwrap() = Some((Instant::now(), s.clone()));
    Ok(s)
}

// ---------- ручки ----------

async fn health(State(app): State<App>) -> Response {
    let n = swarm_get(&app).await.map(|s| s.models.len()).unwrap_or(0);
    Json(json!({"status": "ok", "service": "ntfry-ai", "version": env!("CARGO_PKG_VERSION"), "modelsAvailable": n}))
        .into_response()
}

async fn nstats(State(app): State<App>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&app, &headers) {
        return r;
    }
    let swarm = swarm_get(&app).await.unwrap_or_default();
    Json(json!({
        "keys": app.keys.stats_json(),
        "swarm": {
            "models": swarm.models.len(),
            "callable": swarm.models.iter().filter(|m| !m.gateways.is_empty()).count(),
            "updatedAt": swarm.updated_at,
        },
    }))
    .into_response()
}

async fn models(State(app): State<App>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&app, &headers) {
        return r;
    }
    let swarm = match swarm_get(&app).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &format!("трекеры недоступны: {e}")),
    };
    let data: Vec<serde_json::Value> = swarm
        .models
        .iter()
        .filter(|m| !m.gateways.is_empty())
        .map(|m| {
            json!({
                "id": m.alias,
                "object": "model",
                "created": swarm.updated_at,
                "owned_by": "netfory",
                "x_netfory": {
                    "nodes": m.nodes,
                    "gateways": m.gateways.len(),
                    "tokPerSec": m.tok_per_sec,
                    "pricePerMtok": m.price_per_mtok,
                },
            })
        })
        .collect();
    Json(json!({"object": "list", "data": data})).into_response()
}

// ---------- OpenAI-типы запросов ----------

#[derive(Deserialize)]
struct ChatMsg {
    #[serde(default)]
    role: String,
    #[serde(default)]
    content: serde_json::Value,
}

#[derive(Deserialize)]
struct ChatReq {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    messages: Vec<ChatMsg>,
    #[serde(default)]
    max_tokens: Option<usize>,
    #[serde(default)]
    max_completion_tokens: Option<usize>,
    #[serde(default)]
    stream: bool,
    /// think-режим reasoning-моделей (или суффикс модели ":think")
    #[serde(default)]
    reasoning: Option<bool>,
}

#[derive(Deserialize)]
struct CompReq {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    prompt: serde_json::Value,
    #[serde(default)]
    max_tokens: Option<usize>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    reasoning: Option<bool>,
}

/// content: строка или массив частей [{type:"text", text:…}]
fn content_text(v: &serde_json::Value) -> String {
    if let Some(s) = v.as_str() {
        return s.to_string();
    }
    if let Some(a) = v.as_array() {
        return a
            .iter()
            .filter_map(|p| {
                if p["type"].as_str().unwrap_or("text") == "text" {
                    p["text"].as_str().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    String::new()
}

/// Шлюз оборачивает prompt как ОДНО user-сообщение своим шаблоном чата,
/// поэтому multi-turn сплющиваем в транскрипт (ограничение фазы 1).
fn flatten(messages: &[ChatMsg]) -> String {
    let parts: Vec<(String, String)> = messages
        .iter()
        .map(|m| (m.role.clone(), content_text(&m.content)))
        .filter(|(_, t)| !t.trim().is_empty())
        .collect();
    if parts.len() == 1 && parts[0].0 != "system" {
        return parts[0].1.clone();
    }
    let mut out = String::new();
    for (role, text) in &parts {
        match role.as_str() {
            "system" => {
                out.push_str(text);
                out.push_str("\n\n");
            }
            "assistant" => {
                out.push_str("Assistant: ");
                out.push_str(text);
                out.push('\n');
            }
            _ => {
                out.push_str("User: ");
                out.push_str(text);
                out.push('\n');
            }
        }
    }
    out.trim_end().to_string()
}

/// имя модели → (alias, think-суффикс); "auto"/"" → лучшая вызываемая модель
fn resolve_model(swarm: &net::Swarm, want: Option<&str>) -> Result<(net::SwarmModel, bool), Response> {
    let mut name = want.unwrap_or("auto").trim().to_string();
    let mut think = false;
    if let Some(s) = name.strip_suffix(":think") {
        name = s.to_string();
        think = true;
    }
    let lname = name.to_lowercase();
    let found = if lname.is_empty() || lname == "auto" || lname == "default" || lname == "netfory/auto" {
        swarm.models.iter().find(|m| !m.gateways.is_empty())
    } else {
        swarm.models.iter().find(|m| m.alias.to_lowercase() == lname)
    };
    match found {
        Some(m) if !m.gateways.is_empty() => Ok((m.clone(), think)),
        Some(m) => Err(err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            "api_error",
            &format!("модель «{}» есть в рое, но ни один шлюз сейчас не принимает запросы", m.alias),
        )),
        None => Err(err_json(
            StatusCode::NOT_FOUND,
            "invalid_request_error",
            &format!("модель «{name}» не найдена в рое — список: GET /v1/models"),
        )),
    }
}

// ---------- инференс с failover ----------

async fn start_stream(
    app: &App,
    model: &net::SwarmModel,
    msg: Msg,
) -> Result<(Ev, tokio::sync::mpsc::Receiver<Ev>), String> {
    let mut candidates = model.gateways.clone();
    if candidates.is_empty() {
        // fallback для старых трекеров без irohNodeId в stats
        let trackers = app.cfg.trackers.clone();
        let alias = model.alias.clone();
        if let Ok(Some(id)) =
            tokio::task::spawn_blocking(move || Ok::<_, String>(net::pick_gateway_blocking(&trackers, &alias))).await.map_err(|e| e.to_string())?
        {
            candidates.push(id);
        }
    }
    let mut last_err = "нет доступных шлюзов".to_string();
    for gw in candidates.iter().take(FAILOVER_TRIES) {
        let Ok(id) = gw.parse::<EndpointId>() else {
            last_err = format!("кривой iroh id: {gw}");
            continue;
        };
        let conn = match net::connect(&app.ep, id).await {
            Ok(c) => c,
            Err(e) => {
                last_err = e;
                continue;
            }
        };
        let (mut send, recv) = match conn.open_bi().await {
            Ok(v) => v,
            Err(e) => {
                last_err = e.to_string();
                continue;
            }
        };
        let m = &msg;
        if let Err(e) = net::write_msg(&mut send, m).await {
            last_err = e;
            continue;
        }
        let _ = send.finish();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Ev>(64);
        tokio::spawn(net::pump(conn, send, recv, tx));
        // первое событие решает: стримим или пробуем следующий шлюз
        match tokio::time::timeout(Duration::from_secs(FIRST_TOKEN_SECS), rx.recv()).await {
            Ok(Some(Ev::Error(e))) => {
                warn!("шлюз {gw}: {e} — пробую следующий");
                last_err = e;
            }
            Ok(Some(first)) => {
                info!("инференс пошёл через ai://{gw} (модель {})", model.alias);
                return Ok((first, rx));
            }
            Ok(None) => last_err = "стрим закрылся без ответа".into(),
            Err(_) => last_err = format!("таймаут первого токена ({FIRST_TOKEN_SECS}с)"),
        }
    }
    Err(last_err)
}

// ---------- chat/completions ----------

fn chat_chunk(id: &str, created: u64, model: &str, delta: serde_json::Value, finish: Option<&str>) -> String {
    json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    })
    .to_string()
}

fn rand_id(prefix: &str) -> String {
    use rand::RngCore;
    let mut b = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut b);
    format!("{prefix}-{}", hex::encode(b))
}

async fn chat(State(app): State<App>, headers: HeaderMap, Json(req): Json<ChatReq>) -> Response {
    let key = match auth(&app, &headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let swarm = match swarm_get(&app).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &format!("трекеры недоступны: {e}")),
    };
    let (model, think_suffix) = match resolve_model(&swarm, req.model.as_deref()) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let think = req.reasoning.unwrap_or(think_suffix);
    let turns: Vec<net::ChatTurn> = req
        .messages
        .iter()
        .map(|m| net::ChatTurn { role: m.role.clone(), content: content_text(&m.content) })
        .filter(|t| !t.content.trim().is_empty())
        .collect();
    if turns.is_empty() {
        return err_json(StatusCode::BAD_REQUEST, "invalid_request_error", "messages пуст");
    }
    let max = req.max_completion_tokens.or(req.max_tokens).unwrap_or(app.cfg.default_max_tokens).clamp(1, 2048);
    let prompt_tokens =
        (turns.iter().map(|t| t.content.chars().count() as u64).sum::<u64>() + 3) / 4; // оценка
    // multi-turn / system → родной InferChat (воркер v0.13+, каноничный шаблон
    // чата модели); одиночное user-сообщение — обычный Infer (совместим со всеми)
    let native = turns.len() > 1 || turns[0].role != "user";
    let flat = flatten(&req.messages);
    let msg = if native {
        Msg::InferChat { messages: turns, max_tokens: max, model: Some(model.alias.clone()), think }
    } else {
        Msg::Infer { prompt: flat.clone(), max_tokens: max, model: Some(model.alias.clone()), think }
    };
    let (first, rx) = match start_stream(&app, &model, msg).await {
        Ok(v) => v,
        Err(e) if native => {
            // старый воркер без InferChat — откат на плоский транскрипт
            warn!("InferChat не прошёл ({e}) — пробую плоский Infer (старый воркер?)");
            let fb = Msg::Infer { prompt: flat, max_tokens: max, model: Some(model.alias.clone()), think };
            match start_stream(&app, &model, fb).await {
                Ok(v) => v,
                Err(e) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &format!("рой недоступен: {e}")),
            }
        }
        Err(e) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &format!("рой недоступен: {e}")),
    };
    let id = rand_id("chatcmpl");
    let created = now();
    let alias = model.alias.clone();
    if req.stream {
        stream_response(app, key, id, created, alias, first, rx, prompt_tokens, max, false)
    } else {
        collect_response(app, key, id, created, alias, first, rx, prompt_tokens, max, false).await
    }
}

async fn completions(State(app): State<App>, headers: HeaderMap, Json(req): Json<CompReq>) -> Response {
    let key = match auth(&app, &headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let swarm = match swarm_get(&app).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &format!("трекеры недоступны: {e}")),
    };
    let (model, think_suffix) = match resolve_model(&swarm, req.model.as_deref()) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let prompt = match &req.prompt {
        v if v.is_string() => v.as_str().unwrap_or("").to_string(),
        v if v.is_array() => v.as_array().unwrap().iter().filter_map(|p| p.as_str()).collect::<Vec<_>>().join("\n"),
        _ => String::new(),
    };
    if prompt.is_empty() {
        return err_json(StatusCode::BAD_REQUEST, "invalid_request_error", "prompt пуст");
    }
    let think = req.reasoning.unwrap_or(think_suffix);
    let max = req.max_tokens.unwrap_or(app.cfg.default_max_tokens).clamp(1, 2048);
    let prompt_tokens = (prompt.chars().count() as u64 + 3) / 4;
    let msg = Msg::Infer { prompt, max_tokens: max, model: Some(model.alias.clone()), think };
    let (first, rx) = match start_stream(&app, &model, msg).await {
        Ok(v) => v,
        Err(e) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &format!("рой недоступен: {e}")),
    };
    let id = rand_id("cmpl");
    let created = now();
    let alias = model.alias.clone();
    if req.stream {
        stream_response(app, key, id, created, alias, first, rx, prompt_tokens, max, true)
    } else {
        collect_response(app, key, id, created, alias, first, rx, prompt_tokens, max, true).await
    }
}

// ---------- стриминг SSE ----------

#[allow(clippy::too_many_arguments)]
fn stream_response(
    app: App,
    key: Option<String>,
    id: String,
    created: u64,
    alias: String,
    first: Ev,
    mut rx: tokio::sync::mpsc::Receiver<Ev>,
    prompt_tokens: u64,
    max: usize,
    legacy: bool,
) -> Response {
    let (otx, orx) = tokio::sync::mpsc::channel::<Result<Event, std::convert::Infallible>>(64);
    tokio::spawn(async move {
        let mut completion = 0u64;
        let mut node = String::new();
        let mut ms = 0u64;
        if !legacy {
            let _ = otx
                .send(Ok(Event::default().data(chat_chunk(&id, created, &alias, json!({"role": "assistant", "content": ""}), None))))
                .await;
        }
        let mut ev = Some(first);
        loop {
            let e = match ev.take() {
                Some(e) => e,
                None => match tokio::time::timeout(Duration::from_secs(IDLE_SECS), rx.recv()).await {
                    Ok(Some(e)) => e,
                    Ok(None) => break,
                    Err(_) => Ev::Error(format!("таймаут генерации ({IDLE_SECS}с тишины)")),
                },
            };
            match e {
                Ev::Token(t) => {
                    completion += 1;
                    let data = if legacy {
                        json!({"id": id, "object": "text_completion", "created": created, "model": alias,
                            "choices": [{"index": 0, "text": t, "finish_reason": null}]})
                        .to_string()
                    } else {
                        chat_chunk(&id, created, &alias, json!({"content": t}), None)
                    };
                    if otx.send(Ok(Event::default().data(data))).await.is_err() {
                        return; // клиент отвалился
                    }
                }
                Ev::Done { node: n, ms: m, .. } => {
                    node = n;
                    ms = m;
                    break;
                }
                Ev::Error(err) => {
                    let _ = otx
                        .send(Ok(Event::default().data(json!({"error": {"message": err, "type": "api_error"}}).to_string())))
                        .await;
                    break;
                }
            }
        }
        let finish = if completion >= max as u64 { "length" } else { "stop" };
        let fin = json!({
            "id": id, "object": if legacy { "text_completion" } else { "chat.completion.chunk" },
            "created": created, "model": alias,
            "choices": [if legacy {
                json!({"index": 0, "text": "", "finish_reason": finish})
            } else {
                json!({"index": 0, "delta": {}, "finish_reason": finish})
            }],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion, "total_tokens": prompt_tokens + completion},
            "x_netfory": {"node": node, "ms": ms, "promptTokensEstimated": true},
        });
        let _ = otx.send(Ok(Event::default().data(fin.to_string()))).await;
        let _ = otx.send(Ok(Event::default().data("[DONE]"))).await;
        if let Some(k) = key {
            app.keys.note_usage(&k, prompt_tokens, completion);
        }
    });
    Sse::new(ReceiverStream::new(orx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("ping"))
        .into_response()
}

// ---------- non-stream ----------

#[allow(clippy::too_many_arguments)]
async fn collect_response(
    app: App,
    key: Option<String>,
    id: String,
    created: u64,
    alias: String,
    first: Ev,
    mut rx: tokio::sync::mpsc::Receiver<Ev>,
    prompt_tokens: u64,
    max: usize,
    legacy: bool,
) -> Response {
    let mut out = String::new();
    let mut completion = 0u64;
    let mut node = String::new();
    let mut ms = 0u64;
    let mut ev = Some(first);
    loop {
        let e = match ev.take() {
            Some(e) => e,
            None => match tokio::time::timeout(Duration::from_secs(IDLE_SECS), rx.recv()).await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(_) => return err_json(StatusCode::GATEWAY_TIMEOUT, "api_error", "таймаут генерации"),
            },
        };
        match e {
            Ev::Token(t) => {
                completion += 1;
                out.push_str(&t);
            }
            Ev::Done { node: n, ms: m, text } => {
                node = n;
                ms = m;
                if !text.is_empty() {
                    out = text; // полный текст шлюза авторитетнее (replay при пересборке)
                }
                break;
            }
            Ev::Error(err) => return err_json(StatusCode::BAD_GATEWAY, "api_error", &err),
        }
    }
    if let Some(k) = &key {
        app.keys.note_usage(k, prompt_tokens, completion);
    }
    let finish = if completion >= max as u64 { "length" } else { "stop" };
    let body = if legacy {
        json!({
            "id": id, "object": "text_completion", "created": created, "model": alias,
            "choices": [{"index": 0, "text": out, "finish_reason": finish}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion, "total_tokens": prompt_tokens + completion},
            "x_netfory": {"node": node, "ms": ms, "promptTokensEstimated": true},
        })
    } else {
        json!({
            "id": id, "object": "chat.completion", "created": created, "model": alias,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": out}, "finish_reason": finish}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion, "total_tokens": prompt_tokens + completion},
            "x_netfory": {"node": node, "ms": ms, "promptTokensEstimated": true},
        })
    };
    let mut resp = Json(body).into_response();
    if let Ok(v) = node.parse::<axum::http::HeaderValue>() {
        resp.headers_mut().insert("x-netfory-node", v);
    }
    resp
}
