use clap::Parser;
use redis::{Commands, Client};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Optimized Redis Performance Test Script (Sequential Data)
#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Redis key to set
    #[arg(long, default_value = "test_key")]
    key: String,

    /// Rate of sets per second
    #[arg(long)]
    rate: f64,
}

fn main() {
    let args = Args::parse();

    println!("Starting Redis performance test...");
    println!("Key: {}", args.key);
    println!("Rate: {} sets/sec", args.rate);

    // Connect to Redis
    let client =
        Client::open("redis://127.0.0.1/").expect("Failed to create Redis client");
    let mut con = client
        .get_connection()
        .expect("Failed to connect to Redis");

    // Preloaded random data table
    let preloaded_data = [
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "The quick brown fox jumps over the lazy dog.",
        "Redis is an in-memory data structure store, used as a database, cache, and message broker.",
        "High-performance computing requires efficient memory access patterns.",
        "Unix domain sockets provide efficient interprocess communication.",
    ];

    // Create a cycle iterator to loop through preloaded data sequentially
    let mut data_iter = preloaded_data.iter().cycle();

    let interval = Duration::from_secs_f64(1.0 / args.rate);
    let start_time = Instant::now();
    let mut count: u64 = 0;

    // Set up a flag to catch Ctrl-C
    let running = Arc::new(AtomicBool::new(true));
    let running_ctrlc = running.clone();
    ctrlc::set_handler(move || {
        running_ctrlc.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Main loop: set the Redis key repeatedly at the specified rate
    while running.load(Ordering::SeqCst) {
        // Fetch next data item from preloaded table
        let value = data_iter.next().unwrap();

        // Store in Redis
        //let res: redis::RedisResult<()> =
        //    redis::cmd("SET").arg(&args.key).arg(*value).query(&mut con);
        let res: redis::RedisResult<()> = con.set(&args.key, *value);
        if let Err(e) = res {
            eprintln!("Error: {}", e);
            break;
        }

        count += 1;
        if count % 1000 == 0 {
            let elapsed = start_time.elapsed();
            println!(
                "[{:.2?}] Set {} keys at {} in Redis.",
                elapsed, count, args.key
            );
        }

        // Sleep to maintain the desired rate
        sleep(interval);
    }

    println!("\nTest stopped by user.");
}
