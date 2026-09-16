//! Process termination signal shared by the server and the projection job.
//!
//! Orchestrators stop a container with SIGTERM and only escalate to SIGKILL
//! after a grace period, so both binaries must treat SIGTERM exactly like
//! Ctrl-C: stop taking new work, finish what is in flight, then exit.

/// Resolves once on SIGTERM or Ctrl-C (SIGINT).
///
/// If a signal handler cannot be installed the future logs the failure and
/// never resolves for that signal, so the process keeps serving instead of
/// exiting on a setup error.
pub async fn signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            log::warn!("failed to listen for Ctrl-C: {error}");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    log::warn!("failed to listen for SIGTERM: {error}");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}
