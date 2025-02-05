// Import necessary crates and modules
use redis::{Commands, Client}; // For Redis operations
use serde::{Deserialize, Serialize}; // For serializing/deserializing JSON
use serde_json::Value; // For working with JSON values
use std::collections::HashMap; // For using HashMap data structure
use std::fs; // For file system operations
use std::os::unix::net::{UnixListener, UnixStream}; // For Unix domain sockets
use std::io::{Read, Write}; // For reading from and writing to streams
use std::sync::Arc; // For thread-safe reference counting
use std::thread; // For spawning threads
use regex::Regex; // For regular expression matching
use lazy_static::lazy_static; // For defining static variables initialized at runtime

// Define the Unix socket path
const SOCKET_PATH: &str = "/tmp/redis_proxy.sock";

// Define the structure of incoming requests
#[derive(Deserialize)]
struct Request {
    action: String, // The action to perform (set, del, sadd, srem)
    key: String, // The Redis key
    value: Option<Value>, // The value to store (optional)
}

// Define the structure of responses sent back to clients
#[derive(Serialize)]
struct Response {
    status: String, // Status of the request (ok or error)
    message: String, // Additional message
}

// Define static variables that are initialized lazily
lazy_static! {
    static ref VALID_PRODUCERS: Vec<&'static str> = vec!["DiskUsage", "ModemWatcher", "Psmon", "SerialPort"]; // Valid producers
    static ref VALID_OBJECTS: Vec<&'static str> = vec!["object1", "object2"]; // Valid objects
    static ref KEY_PATTERN: Regex = generate_key_pattern(); // Compiled regex pattern for key validation
    static ref SCHEMAS: HashMap<&'static str, serde_json::Value> = { // JSON schemas for validating values
        let mut m = HashMap::new();
        m.insert("cs:DiskUsage:object1", serde_json::json!({
            "type": "object",
            "properties": {
                "version": {"type": "number"},
                "disk": {"type": "string"},
                "usage": {"type": "number"}
            },
            "required": ["version", "disk", "usage"]
        }));
        m.insert("cs:ModemWatcher:object2", serde_json::json!({
            "type": "object",
            "properties": {
                "version": {"type": "number"},
                "status": {"type": "string"},
                "signal_strength": {"type": "integer"}
            },
            "required": ["version", "status", "signal_strength"]
        }));
        m
    };
}

// Function to generate the key validation regex pattern
fn generate_key_pattern() -> Regex {
    let producers = VALID_PRODUCERS.join("|"); // Join producers with |
    let objects = VALID_OBJECTS.join("|"); // Join objects with |
    Regex::new(&format!(
        r"^cs:(?P<producer>{}):(?P<object>{})(?::(?P<id>[\w\d]+))?(?::(?P<function>\w+))?$",
        producers, objects
    ))
    .unwrap() // Panic if regex compilation fails
}

// Function to check if a key matches the valid pattern
fn is_valid_key(key: &str) -> bool {
    KEY_PATTERN.is_match(key)
}

// Function to validate a JSON value against the schema for the given key
fn validate_json_schema(key: &str, value: &Value) -> Result<(), String> {
    let base_key = key.splitn(4, ':').take(3).collect::<Vec<&str>>().join(":"); // Extract base key
    if let Some(schema) = SCHEMAS.get(base_key.as_str()) {
        jsonschema::JSONSchema::compile(schema)
            .map_err(|e| e.to_string())? // Compile schema or return error
            .validate(value)
            .map_err(|errors| {
                errors.map(|e| e.to_string()).collect::<Vec<String>>().join(", ") // Collect validation errors
            })?;
    }
    Ok(()) // Return Ok if validation passes
}

// Function to handle an individual request
fn handle_request(redis_client: &mut redis::Connection, data: &str) -> String {
    let request: Result<Request, _> = serde_json::from_str(data); // Deserialize JSON request
    if let Ok(req) = request {
        if !is_valid_key(&req.key) { // Validate key format
            return serde_json::to_string(&Response {
                status: "error".to_string(),
                message: "Invalid key format".to_string(),
            }).unwrap();
        }

        if let Some(ref value) = req.value { // If value exists, validate against schema
            if let Err(err) = validate_json_schema(&req.key, value) {
                return serde_json::to_string(&Response {
                    status: "error".to_string(),
                    message: err,
                }).unwrap();
            }
        }

        // Match the action and perform corresponding Redis command
        let result = match req.action.as_str() {
            "set" => {
                let val = req.value.unwrap_or(Value::Null).to_string();
                redis_client.set::<&str, String, ()>(&req.key, val.clone())
                .and_then(|_| redis_client.publish::<&str, String, ()>(&req.key, format!("set: {}", val)))
            },
            "del" => redis_client.del::<&str, ()>(&req.key)
                .and_then(|_| redis_client.publish::<&str, &str, ()>(&req.key, "del")),
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
            _ => return serde_json::to_string(&Response { // Handle invalid actions
                status: "error".to_string(),
                message: "Invalid action".to_string(),
            }).unwrap(),
        };

        // Return success or error response based on Redis operation result
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
        // Return error if request format is invalid
        serde_json::to_string(&Response {
            status: "error".to_string(),
            message: "Invalid request format".to_string(),
        }).unwrap()
    }
}

// Function to handle client connections
fn handle_client(mut stream: UnixStream, redis_client: Arc<Client>) {
    let mut buffer = Vec::new(); // Buffer to read incoming data
    let mut conn = redis_client.get_connection().expect("Failed to connect to Redis"); // Get Redis connection

    loop {
        let mut temp_buffer = [0; 1024]; // Temporary buffer to read data in chunks
        match stream.read(&mut temp_buffer) {
            Ok(0) => break, // Connection closed by client
            Ok(size) => {
                buffer.extend_from_slice(&temp_buffer[..size]); // Append new data to the buffer
                while let Some(pos) = buffer.iter().position(|&b| b == b'\n') { // Check for complete message (newline-delimited)
                    let line = buffer.drain(..=pos).collect::<Vec<u8>>(); // Extract complete message
                    if let Ok(data) = String::from_utf8(line) {
                        let response = handle_request(&mut conn, data.trim()); // Process the request
                        stream.write_all(response.as_bytes()).unwrap(); // Send response
                    }
                }
            }
            Err(err) => {
                eprintln!("Failed to read from client: {}", err);
                break;
            }
        }
    }
}


// Main function to start the proxy service
fn main() -> std::io::Result<()> {
    if fs::metadata(SOCKET_PATH).is_ok() { // Check if socket file exists
        fs::remove_file(SOCKET_PATH)?; // Remove existing socket file
    }

    let listener = UnixListener::bind(SOCKET_PATH)?; // Bind to the Unix socket path
    println!("Redis Proxy Service Started. Waiting for connections...");

    let redis_client = Arc::new(Client::open("redis://127.0.0.1/").expect("Failed to create Redis client")); // Create Redis client wrapped in Arc

    // Loop to accept incoming connections
    for stream in listener.incoming() {
        match stream {
            Ok(socket) => {
                let client_clone = Arc::clone(&redis_client); // Clone the Redis client for the new thread
                thread::spawn(move || handle_client(socket, client_clone)); // Spawn a new thread to handle the client
            }
            Err(err) => eprintln!("Connection failed: {}", err), // Print error if connection fails
        }
    }

    Ok(()) // Return Ok to indicate successful execution
}
