//! Generate a manifest with many partitions for loading BSBM offer data to Redshift

use serde::Serialize;

#[derive(Debug, Serialize)]
struct ManifestEntry {
    endpoint: String,
    command: String,
    mandatory: bool,
    username: String,
}

impl ManifestEntry {
    pub fn new(scale: i64, min: i64, max: i64) -> ManifestEntry {
        ManifestEntry {
            endpoint: "ec2-34-201-30-211.compute-1.amazonaws.com".to_string(),
            command: format!(
                "starbench/bibm/redshift_csv_wrapper.sh {} {} {}",
                scale, min, max
            ),
            mandatory: true,
            username: "ec2-user".to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Manifest {
    entries: Vec<ManifestEntry>,
}

fn main() {
    let scale: i64 = 100 * 2848_000;
    let base_scale: i64 = 2848;
    let records_per_base_scale = 56960;
    let total_records = (scale / base_scale) * records_per_base_scale;
    let manifest_entries = 192; // currently 4 threads on Redshift so multiples of 4 work best here
    let records_per_entry = total_records / manifest_entries;
    let entries: Vec<_> = (0..manifest_entries)
        .map(|entry_no| {
            let max: i64 = if entry_no == manifest_entries - 1 {
                -1
            } else {
                (entry_no + 1) * records_per_entry - 1
            };
            ManifestEntry::new(scale, entry_no * records_per_entry, max)
        })
        .collect();
    let manifest = Manifest { entries };
    println!("{}", serde_json::to_string_pretty(&manifest).unwrap());
}
