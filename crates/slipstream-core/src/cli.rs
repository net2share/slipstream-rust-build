use std::fmt::Display;
use tracing_subscriber::EnvFilter;

pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();
}

pub fn unwrap_or_exit<T, E>(result: Result<T, E>, context: &str, code: i32) -> T
where
    E: Display,
{
    result.unwrap_or_else(|err| exit_with_error(context, err, code))
}

pub fn exit_with_error(context: &str, err: impl Display, code: i32) -> ! {
    tracing::error!("{}: {}", context, err);
    std::process::exit(code);
}

pub fn exit_with_message(message: &str, code: i32) -> ! {
    tracing::error!("{}", message);
    std::process::exit(code);
}
