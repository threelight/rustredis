use sysinfo::Disks;
use redis::{Commands, Connection};
use serde::{Serialize, Deserialize};
use std::{collections::HashMap, thread, time::{Duration, SystemTime, UNIX_EPOCH}};

#[derive(Serialize, Deserialize)]
struct DiskInfo {
	_timestamp: u128,
    total_space: u64,
    free_space: u64,
    path: String,
}

fn get_disk_space() -> HashMap<String, String> {
	let disks = Disks::new_with_refreshed_list();

    let mut disk_map = HashMap::new();
	let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    for disk in &disks {
        let disk_info = DiskInfo {
			_timestamp: timestamp,
            total_space: disk.total_space(),
            free_space: disk.available_space(),
            path: disk.mount_point().to_string_lossy().to_string(),
        };

        if let Ok(json_str) = serde_json::to_string(&disk_info) {
            disk_map.insert(disk_info.path.clone(), json_str);
        }
    }

    disk_map
}

fn store_in_redis(disk_map: HashMap<String, String>) -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con: Connection = client.get_connection()?;

    for (mount_point, json_value) in disk_map {
        let json_value_clone = json_value.clone();
        let _: () = con.hset("system_disk_space", mount_point, json_value)?;
        let _: () = con.publish("system_disk_space", json_value_clone)?;
    }

    Ok(())
}

fn main() {
    loop {
        println!("Fetching disk space information...");
        let disk_data = get_disk_space();

        match store_in_redis(disk_data) {
            Ok(_) => println!("Disk data stored in Redis successfully."),
            Err(e) => eprintln!("Error storing disk data in Redis: {:?}", e),
        }

        // Wait for 5 minutes
        thread::sleep(Duration::from_secs(300));
    }
}
