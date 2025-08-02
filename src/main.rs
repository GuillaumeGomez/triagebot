#![allow(clippy::new_without_default)]

use anyhow::Context as _;
use axum::Router;
use axum::handler::HandlerWithoutStateExt;
use http_body_util::{combinators::BoxBody, BodyExt};
use bytes::Bytes;
use futures::StreamExt;
use futures::future::FutureExt;
use hyper::body::Frame;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper::server::conn::http1;
use std::time::Duration;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::{task, time};
use tower::{Service, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing as log;
use tracing::Instrument;
use triagebot::gha_logs::GitHubActionLogsCache;
use triagebot::handlers::pr_tracking::ReviewerWorkqueue;
use triagebot::handlers::pr_tracking::load_workqueue;
use triagebot::jobs::{
    JOB_PROCESSING_CADENCE_IN_SECS, JOB_SCHEDULING_CADENCE_IN_SECS, default_jobs,
};
use triagebot::team_data::TeamClient;
use triagebot::zulip::client::ZulipClient;
use triagebot::{EventName, db, github, handlers::Context, notification_listing, payload};

async fn handle_agenda_request(req: String) -> anyhow::Result<String> {
    if req == "/agenda/lang/triage" {
        return triagebot::agenda::lang().call().await;
    }
    if req == "/agenda/lang/planning" {
        return triagebot::agenda::lang_planning().call().await;
    }
    if req == "/agenda/types/planning" {
        return triagebot::agenda::types_planning().call().await;
    }

    anyhow::bail!("Unknown agenda; see /agenda for index.")
}

async fn serve_req(
    req: Request<BoxBody<Bytes, hyper::Error>>,
    ctx: Arc<Context>,
    mut agenda: impl Service<String, Response = String, Error = tower::BoxError>,
) -> Result<Response<BoxBody<Bytes, std::convert::Infallible>>, hyper::Error> {
    log::info!("request = {:?}", req);
    let mut router = route_recognizer::Router::new();
    router.add("/triage", "index".to_string());
    router.add("/triage/:owner/:repo", "pulls".to_string());
    router.add("/gha-logs/:owner/:repo/:log-id", "gha-logs".to_string());

    if let Ok(matcher) = router.recognize(req.uri().path()) {
        if matcher.handler().as_str() == "pulls" {
            let params = matcher.params();
            let owner = params.find("owner");
            let repo = params.find("repo");
            return triagebot::triage::pulls(ctx, owner.unwrap(), repo.unwrap()).await;
        } else if matcher.handler().as_str() == "index" {
            return triagebot::triage::index();
        } else if matcher.handler().as_str() == "gha-logs" {
            let params = matcher.params();
            let owner = params.find("owner").unwrap();
            let repo = params.find("repo").unwrap();
            let log_id = params.find("log-id").unwrap();
            return triagebot::gha_logs::gha_logs(ctx, owner, repo, log_id).await;
        }
    }

    if req.uri().path() == triagebot::gha_logs::ANSI_UP_URL {
        return triagebot::gha_logs::ansi_up_min_js();
    }
    if req.uri().path() == triagebot::gha_logs::SUCCESS_URL {
        return triagebot::gha_logs::success_svg();
    }
    if req.uri().path() == triagebot::gha_logs::FAILURE_URL {
        return triagebot::gha_logs::failure_svg();
    }

    if req.uri().path() == "/agenda" {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body(triagebot::agenda::INDEX.to_string().boxed())
            .unwrap());
    }
    if req.uri().path() == "/agenda/lang/triage"
        || req.uri().path() == "/agenda/lang/planning"
        || req.uri().path() == "/agenda/types/planning"
    {
        match agenda
            .ready()
            .await
            .expect("agenda keeps running")
            .call(req.uri().path().to_owned())
            .await
        {
            Ok(agenda) => {
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(agenda.boxed())
                    .unwrap());
            }
            Err(err) => {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(err.to_string().boxed())
                    .unwrap());
            }
        }
    }

    if req.uri().path() == "/" {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body("Triagebot is awaiting triage.".to_string().boxed())
            .unwrap());
    }
    if req.uri().path() == "/bors-commit-list" {
        let res = db::rustc_commits::get_commits_with_artifacts(&*ctx.db.get().await).await;
        let res = match res {
            Ok(r) => r,
            Err(e) => {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(format!("{e:?}").boxed())
                    .unwrap());
            }
        };
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&res).unwrap().boxed())
            .unwrap());
    }
    if req.uri().path() == "/notifications" {
        if let Some(query) = req.uri().query() {
            let user = url::form_urlencoded::parse(query.as_bytes()).find(|(k, _)| k == "user");
            if let Some((_, name)) = user {
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(notification_listing::render(&ctx.db.get().await, &*name).await.boxed())
                    .unwrap());
            }
        }

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body("Please provide `?user=<username>` query param on URL.".to_string().boxed())
            .unwrap());
    }
    if req.uri().path() == "/zulip-hook" {
        let mut whole_body = req.collect().await?.to_bytes();

        log::info!("/zulip-hook request body: {whole_body:?}");
        let req = match serde_json::from_slice(&whole_body) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(format!("Did not send valid JSON request: {e}").boxed())
                    .unwrap());
            }
        };

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(triagebot::zulip::respond(ctx, req).await.boxed())
            .unwrap());
    }
    if req.uri().path() != "/github-hook" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(String::new().boxed())
            .unwrap());
    }
    if req.method() != hyper::Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header(header::ALLOW, "POST")
            .body(String::new().boxed())
            .unwrap());
    }
    let event = if let Some(ev) = req.headers().get("X-GitHub-Event") {
        let ev = match ev.to_str().ok() {
            Some(v) => v,
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("X-GitHub-Event header must be UTF-8 encoded".to_string().boxed())
                    .unwrap());
            }
        };
        match ev.parse::<EventName>() {
            Ok(v) => v,
            Err(_) => unreachable!(),
        }
    } else {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("X-GitHub-Event header must be set".to_string().boxed())
            .unwrap());
    };
    log::debug!("event={}", event);
    let signature = if let Some(sig) = req.headers().get("X-Hub-Signature-256") {
        match sig.to_str().ok() {
            Some(v) => v.to_string(),
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("X-Hub-Signature-256 header must be UTF-8 encoded".to_string().boxed())
                    .unwrap());
            }
        }
    } else {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("X-Hub-Signature-256 header must be set".to_string().boxed())
            .unwrap());
    };
    log::debug!("signature={}", signature);

    let mut whole_body = req.collect().await?.to_bytes();

    if let Err(_) = payload::assert_signed(&signature, &whole_body) {
        return Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body("Wrong signature".to_string().boxed())
            .unwrap());
    }
    let payload = match str::from_utf8(&whole_body) {
        Ok(p) => p,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Payload must be UTF-8".to_string().boxed())
                .unwrap());
        }
    };

    match triagebot::webhook(event, payload, &ctx).await {
        Ok(true) => Ok(Response::new("processed request".to_string().boxed())),
        Ok(false) => Ok(Response::new("ignored request".to_string().boxed())),
        Err(err) => {
            log::error!("request failed: {:?}", err);
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(format!("request failed: {err:?}").boxed())
                .unwrap())
        }
    }
}

async fn run_server(addr: SocketAddr) -> anyhow::Result<()> {
    let gh = github::GithubClient::new_from_env();
    let zulip = ZulipClient::new_from_env();
    let team_api = TeamClient::new_from_env();
    let oc = octocrab::OctocrabBuilder::new()
        .personal_token(github::default_token_from_env())
        .build()
        .expect("Failed to build octocrab.");

    // Loading the workqueue takes ~10-15s, and it's annoying for local rebuilds.
    // Allow users to opt out of it.
    let skip_loading_workqueue = env::var("SKIP_WORKQUEUE")
        .ok()
        .map(|v| v == "1")
        .unwrap_or(false);

    // Load the initial workqueue state from GitHub
    // In case this fails, we do not want to block triagebot, instead
    // we use an empty workqueue and let it be updated later through
    // webhooks and the `PullRequestAssignmentUpdate` cron job.
    let workqueue = if skip_loading_workqueue {
        tracing::warn!("Skipping workqueue loading");
        ReviewerWorkqueue::default()
    } else {
        tracing::info!("Loading reviewer workqueue for rust-lang/rust");
        let workqueue =
            match tokio::time::timeout(Duration::from_secs(60), load_workqueue(&oc)).await {
                Ok(Ok(workqueue)) => workqueue,
                Ok(Err(error)) => {
                    tracing::error!("Cannot load initial workqueue: {error:?}");
                    ReviewerWorkqueue::default()
                }
                Err(_) => {
                    tracing::error!("Cannot load initial workqueue, timeouted after a minute");
                    ReviewerWorkqueue::default()
                }
            };
        tracing::info!("Workqueue loaded");
        workqueue
    };

    // Only run the migrations after the workqueue has been loaded, immediately
    // before starting the HTTP server.
    // On AWS ECS, triagebot shortly runs in two instances at once.
    // We thus want to minimize the time where migrations have been executed
    // and the old instance potentially runs on an newer database schema.
    let db_url = std::env::var("DATABASE_URL").expect("needs DATABASE_URL");
    let pool = db::ClientPool::new(db_url.clone());
    if !std::env::var("SKIP_DB_MIGRATIONS").is_ok_and(|value| value == "1") {
        db::run_migrations(&mut *pool.get().await)
            .await
            .context("database migrations")?;
    }

    let ctx = Arc::new(Context {
        username: std::env::var("TRIAGEBOT_USERNAME").or_else(|err| match err {
            std::env::VarError::NotPresent => Ok("rustbot".to_owned()),
            err => Err(err),
        })?,
        db: pool,
        github: gh,
        team: team_api,
        octocrab: oc,
        workqueue: Arc::new(RwLock::new(workqueue)),
        gha_logs: Arc::new(RwLock::new(GitHubActionLogsCache::default())),
        zulip,
    });

    // Run all jobs that have a schedule (recurring jobs)
    if !is_scheduled_jobs_disabled() {
        spawn_job_scheduler(db_url);
        spawn_job_runner(ctx.clone());
    }

    let agenda = tower::ServiceBuilder::new()
        .buffer(10)
        .layer_fn(|input| {
            tower::util::MapErr::new(
                tower::load_shed::LoadShed::new(tower::limit::RateLimit::new(
                    input,
                    tower::limit::rate::Rate::new(2, std::time::Duration::from_secs(60)),
                )),
                |e| {
                    tracing::error!("agenda request failed: {:?}", e);
                    anyhow::anyhow!("Rate limit of 2 request / 60 seconds exceeded")
                },
            )
        })
        .service_fn(handle_agenda_request);

    let app = Router::new()
        .layer(axum::middleware::from_fn(|request: Request<_>, next: axum::middleware::Next| async move {
            let req_id = request.headers().get("x-request-id").cloned();
            let span = tracing::span!(tracing::Level::INFO, "request", ?req_id);
            let log_info_response = matches!(request.uri().path(), "/github-hook" | "/zulip-hook");
            let ctx = ctx.clone();
            let agenda = agenda.clone();
            serve_req(request, ctx.clone(), agenda.clone())
                .map(move |mut resp: Result<_, _>| {
                    if log_info_response {
                        log::info!("response = {resp:?}");
                    } else {
                        log::debug!("response = {resp:?}");
                    }
                    resp
                })
                .instrument(span)
                .await
        }));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    log::info!("Listening on http://{}", addr);
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

/// Spawns a background tokio task which runs continuously to queue up jobs
/// to be run by the job runner.
///
/// The scheduler wakes up every `JOB_SCHEDULING_CADENCE_IN_SECS` seconds to
/// check if there are any jobs ready to run. Jobs get inserted into the the
/// database which acts as a queue.
fn spawn_job_scheduler(db_url: String) {
    task::spawn(async move {
        loop {
            let db_url = db_url.clone();
            let res = task::spawn(async move {
                let pool = db::ClientPool::new(db_url);
                let mut interval =
                    time::interval(time::Duration::from_secs(JOB_SCHEDULING_CADENCE_IN_SECS));

                loop {
                    interval.tick().await;
                    db::schedule_jobs(&*pool.get().await, default_jobs())
                        .await
                        .context("database schedule jobs")
                        .unwrap();
                }
            });

            match res.await {
                Err(err) if err.is_panic() => {
                    /* handle panic in above task, re-launching */
                    tracing::error!("schedule_jobs task died (error={err})");
                    tokio::time::sleep(std::time::Duration::new(5, 0)).await;
                }
                _ => unreachable!(),
            }
        }
    });
}

/// Spawns a background tokio task which runs continuously to run scheduled
/// jobs.
///
/// The runner wakes up every `JOB_PROCESSING_CADENCE_IN_SECS` seconds to
/// check if any jobs have been put into the queue by the scheduler. They
/// will get popped off the queue and run if any are found.
fn spawn_job_runner(ctx: Arc<Context>) {
    task::spawn(async move {
        loop {
            let ctx = ctx.clone();
            let res = task::spawn(async move {
                let mut interval =
                    time::interval(time::Duration::from_secs(JOB_PROCESSING_CADENCE_IN_SECS));

                loop {
                    interval.tick().await;
                    db::run_scheduled_jobs(&ctx)
                        .await
                        .context("run database scheduled jobs")
                        .unwrap();
                }
            });

            match res.await {
                Err(err) if err.is_panic() => {
                    /* handle panic in above task, re-launching */
                    tracing::error!("run_scheduled_jobs task died (error={err})");
                    tokio::time::sleep(std::time::Duration::new(5, 0)).await;
                }
                _ => unreachable!(),
            }
        }
    });
}

/// Determines whether or not background scheduled jobs should be disabled for
/// the purpose of testing.
///
/// This helps avoid having random jobs run while testing other things.
fn is_scheduled_jobs_disabled() -> bool {
    env::var_os("TRIAGEBOT_TEST_DISABLE_JOBS").is_some()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(std::env::var_os("DISABLE_COLOR").is_none())
        .try_init()
        .unwrap();

    let port = env::var("PORT")
        .ok()
        .map(|p| p.parse::<u16>().expect("parsed PORT"))
        .unwrap_or(8000);
    let addr = ([0, 0, 0, 0], port).into();
    run_server(addr).await.context("Failed to run the server")?;
    Ok(())
}
