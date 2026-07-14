use codex_arg0::Arg0DispatchPaths;
use codex_arg0::arg0_dispatch_or_else;

const BOOTSTRAPPED_ENV: &str = "CODEX_WORKER_ADAPTER_BOOTSTRAPPED";

fn main() -> anyhow::Result<()> {
    if std::env::var_os(BOOTSTRAPPED_ENV).is_some() {
        return arg0_dispatch_or_else(|_arg0_paths: Arg0DispatchPaths| async move {
            anyhow::bail!("worker adapter helper re-entered the normal worker entrypoint")
        });
    }

    let bootstrap = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(codex_worker_adapter::bootstrap())?
    };
    // The temporary bootstrap runtime has been dropped and arg0_dispatch_or_else has not created
    // the multithreaded worker runtime yet, so no other thread can access the process environment.
    unsafe {
        std::env::set_var("CODEX_HOME", bootstrap.codex_home());
        std::env::set_var(BOOTSTRAPPED_ENV, "1");
    }
    arg0_dispatch_or_else(move |arg0_paths: Arg0DispatchPaths| async move {
        codex_worker_adapter::run(arg0_paths, bootstrap).await
    })
}
