use lambda_runtime::{service_fn, Error, LambdaEvent};
use quickwit_serve::BuildInfo;
use serde_json::{json, Value};

pub async fn run() -> Result<(), Error> {
    let func = service_fn(func);
    lambda_runtime::run(func).await
}

async fn func(_event: LambdaEvent<Value>) -> Result<Value, Error> {
    Ok(json!({
        "message": format!("Hello from Quickwit {}!", BuildInfo::get().version)
    }))
}
