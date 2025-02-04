use redis::{Commands, Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::os::unix::net::{UnixListener, UnixStream};
use std::io::{Read, Write};
use std::sync::Arc;
use std::thread;
use regex::Regex;
use lazy_static::lazy_static;

const SOCKET_PATH: &str = "/tmp/redis_proxy.sock";

#[derive(Deserialize)]
struct Request {
    action: String,
    key: String,
    value: Option<Value>,
}

#[derive(Serialize)]
struct Response {
    status: String,
    message: String,
}

lazy_static! {
    static ref VALID_PRODUCERS: Vec<&'static str> = vec!["DiskUsage", "ModemWatcher", "Psmon", "SerialPort"];
    static ref VALID_OBJECTS: Vec<&'static str> = vec!["object1", "object2"];
    static ref KEY_PATTERN: Regex = generate_key_pattern();
    static ref SCHEMAS: HashMap<&'static str, serde_json::Value> = {
        let mut m = HashMap::new();
        m.insert("cs:DiskUsage:object1", serde_json::json!({
            "type": "object",
            "properties": {
                "disk": {"type": "string"},
                "usage": {"type": "number"}
            },
            "required": ["disk", "usage"]
        }));
        m.insert("cs:ModemWatcher:object2", serde_json::json!({
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "signal_strength": {"type": "integer"}
            },
            "required": ["status", "signal_strength"]
        }));
        m
    };
}

fn generate_key_pattern() -> Regex {
    let producers = VALID_PRODUCERS.join("|");
    let objects = VALID_OBJECTS.join("|");
    Regex::new(&format!(
        r"^cs:(?P<producer>{}):(?P<object>{})(?::(?P<id>[\w\d]+))?(?::(?P<function>\w+))?$",
        producers, objects
    ))
    .unwrap()
}

fn is_valid_key(key: &str) -> bool {
    KEY_PATTERN.is_match(key)
}

fn validate_json_schema(key: &str, value: &Value) -> Result<(), String> {
    let base_key = key.splitn(4, ':').take(3).collect::<Vec<&str>>().join(":");
    if let Some(schema) = SCHEMAS.get(base_key.as_str()) {
        jsonschema::JSONSchema::compile(schema)
            .map_err(|e| e.to_string())?
            .validate(value)
            .map_err(|errors| {
            errors.map(|e| e.to_string()).collect::<Vec<String>>().join(", ")
        })?;
    }
    Ok(())
}

fn handle_request(redis_client: &mut redis::Connection, data: &str) -> String {
    let request: Result<Request, _> = serde_json::from_str(data);
    if let Ok(req) = request {
        if !is_valid_key(&req.key) {
            return serde_json::to_string(&Response {
                status: "error".to_string(),
                message: "Invalid key format".to_string(),
            }).unwrap();
        }

        if let Some(ref value) = req.value {
            if let Err(err) = validate_json_schema(&req.key, value) {
                return serde_json::to_string(&Response {
                    status: "error".to_string(),
                    message: err,
                }).unwrap();
            }
        }

        let result = match req.action.as_str() {
            "set" => {
                let val = req.value.unwrap_or(Value::Null).to_string();
                redis_client.set::<&str, String, ()>(&req.key, val.clone())
                .and_then(|_| redis_client.publish::<&str, String, ()>(&req.key, format!("set: {}", val)))
            },
            "del" => redis_client.del::<&str, ()>(&req.key),
            "sadd" => {
                let val = req.value.unwrap_or(Value::Null).to_string();
                redis_client.sadd::<&str, String, ()>(&req.key, val.clone())
                    .and_then(|_| redis_client.publish::<&str, String, ()>(&req.key, format!("sadd: {}", val)))
            },
            "srem" => {
                let val = req.value.unwrap_or(Value::Null).to_string();
                redis_client.srem::<&str, String, ()>(&req.key, val.clone())
                    .and_then(|_| redis_client.publish::<&str, String, ()>(&req.key, format!("srem: {}", val)))
            },
            _ => return serde_json::to_string(&Response {
                status: "error".to_string(),
                message: "Invalid action".to_string(),
            }).unwrap(),
        };

        match result {
            Ok(_) => serde_json::to_string(&Response {
                status: "ok".to_string(),
                message: "Action completed successfully".to_string(),
            }).unwrap(),
            Err(err) => serde_json::to_string(&Response {
                status: "error".to_string(),
                message: err.to_string(),
            }).unwrap(),
        }
    } else {
        serde_json::to_string(&Response {
            status: "error".to_string(),
            message: "Invalid request format".to_string(),
        }).unwrap()
    }
}

fn handle_client(mut stream: UnixStream, redis_client: Arc<Client>) {
    let mut buffer = [0; 1024];
    match stream.read(&mut buffer) {
        Ok(size) if size > 0 => {
            let data = String::from_utf8_lossy(&buffer[..size]);
            let mut conn = redis_client.get_connection().expect("Failed to connect to Redis");
            let response = handle_request(&mut conn, &data);
            stream.write_all(response.as_bytes()).unwrap();
        }
        _ => {}
    }
}

fn main() -> std::io::Result<()> {
    if fs::metadata(SOCKET_PATH).is_ok() {
        fs::remove_file(SOCKET_PATH)?;
    }

    let listener = UnixListener::bind(SOCKET_PATH)?;
    println!("Redis Proxy Service Started. Waiting for connections...");

    let redis_client = Arc::new(Client::open("redis://127.0.0.1/").expect("Failed to create Redis client"));

    for stream in listener.incoming() {
        match stream {
            Ok(socket) => {
                let client_clone = Arc::clone(&redis_client);
                thread::spawn(move || handle_client(socket, client_clone));
            }
            Err(err) => eprintln!("Connection failed: {}", err),
        }
    }

    Ok(())
}
