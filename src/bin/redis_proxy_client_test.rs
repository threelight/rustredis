use std::os::unix::net::UnixStream;
use std::io::{Write, Read};
use std::thread;
use std::time::Duration;
use clap::Parser;
use serde_json::json;

/// Redis Proxy Test Client
#[derive(Parser)]
struct Args {
    /// Key to set in Redis
    #[arg(long)]
    key: String,

    /// Number of requests per second
    #[arg(long)]
    rate: u64,
}

fn main() {
    let args = Args::parse();

    // Define the Unix socket path used by the Redis Proxy
    let socket_path = "/tmp/redis_proxy.sock";

    // Connect to the Redis Proxy
    let mut stream = UnixStream::connect(&socket_path)
        .expect("Failed to connect to Redis Proxy");

    println!("Connected to Redis Proxy. Sending {} requests per second to key: {}", args.rate, args.key);

    let interval = Duration::from_secs_f64(1.0 / args.rate as f64);
    let mut usage_value = 1; // Initialize usage value

    loop {
        // Constructing a JSON request matching the schema
        let request = json!({
            "action": "set",
            "key": args.key,
            "value": {
                "version": 1.0,
                "disk": "/dev/sda1",
                "usage": usage_value
            }
        });

        // Increment usage value and reset if it exceeds 10000
        usage_value = if usage_value >= 10000 { 1 } else { usage_value + 1 };

        // Send request
        let request_str = format!("{}\n", request.to_string());
        stream.write_all(request_str.as_bytes())
            .expect("Failed to send request");

        // Read and print response
        let mut buffer = [0; 512];
        if let Ok(size) = stream.read(&mut buffer) {
            if size > 0 {
                let response = String::from_utf8_lossy(&buffer[..size]);
                println!("Received response: {}", response);
            }
        }

        // Wait before sending the next request
        thread::sleep(interval);
    }
}