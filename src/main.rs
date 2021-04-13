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

struct GeneratorConfig {
    scale: i64,
    // currently 4 threads on Redshift so multiples of 4 work best here
    manifest_entries: i64,
}

fn main() {
    let trillion_scale_config = GeneratorConfig {
        scale: 1000 * 2848_000,
        // remember that each time we invoke the generator process, it must generate products
        // and offers to reach the specified offset. there are no shortcuts (could't we save the
        // seeds at specific intervals?)

        // use of the #12 gives us a reasonable balance between waste and being able to get useful
        // data sooner (by commenting out entries and loading only the first few at a time)
        manifest_entries: 12,
    };

    let config = trillion_scale_config;

    // scan for 1M
    let base_scale: i64 = 2848;
    let records_per_base_scale = 56960;
    let total_records = (config.scale / base_scale) * records_per_base_scale;
    let records_per_entry = total_records / config.manifest_entries;
    let entries: Vec<_> = (0..config.manifest_entries)
        .map(|entry_no| {
            let max: i64 = if entry_no == config.manifest_entries - 1 {
                -1
            } else {
                (entry_no + 1) * records_per_entry - 1
            };
            ManifestEntry::new(config.scale, entry_no * records_per_entry, max)
        })
        .collect();
    let manifest = Manifest { entries };
    println!("{}", serde_json::to_string_pretty(&manifest).unwrap());
}
