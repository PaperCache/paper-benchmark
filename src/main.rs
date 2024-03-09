mod access;

use std::time::Instant;
use clap::Parser;

use kwik::{
	fmt,
	FileReader,
	progress::{Progress, Tag},
	binary_reader::{BinaryReader, SizedChunk},
};

use paper_client::PaperClient;
use crate::access::{Access, Command};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
	#[arg(short, long, default_value = "127.0.0.1")]
	host: String,

	#[arg(short, long, default_value_t = 3145)]
	port: u32,

	#[arg(short, long)]
	trace_path: String,
}

#[derive(Default)]
struct Stats {
	num_gets: u64,
	num_sets: u64,

	total_get_time: u64,
	total_set_time: u64,
}

fn main() {
	let args = Args::parse();

	let Ok(mut client) = PaperClient::new(&args.host, args.port) else {
		println!("Could not connect to host.");
		return;
	};

	client.wipe().unwrap();

	let reader = BinaryReader::<Access>::new(&args.trace_path)
		.expect("Invalid trace path.");

	println!("Processing {} accesses", fmt::number(reader.size() / Access::size() as u64));

	let mut progress = Progress::new(reader.size(), &[
		Tag::Tps,
		Tag::Eta,
		Tag::Time,
	]);

	let mut stats = Stats::default();

	for access in reader {
		match access.command {
			Command::Get => {
				let start_time = Instant::now();
				client.get(&access.key).unwrap();
				let elapsed_time = start_time.elapsed().as_micros() as u64;

				stats.num_gets += 1;
				stats.total_get_time += elapsed_time;
			},

			Command::Set => {
				let start_time = Instant::now();
				client.set(&access.key, &access.value, access.ttl).unwrap();
				let elapsed_time = start_time.elapsed().as_micros() as u64;

				stats.num_sets += 1;
				stats.total_set_time += elapsed_time;
			},
		};

		progress.tick(Access::size());
	}

	let get_rate = stats.num_gets as f64 / (stats.total_get_time / 1_000_000) as f64;
	let set_rate = stats.num_sets as f64 / (stats.total_set_time / 1_000_000) as f64;

	println!();
	println!("GET accesses/sec: {}", fmt::number(get_rate as u64));
	println!("SET accesses/sec: {}", fmt::number(set_rate as u64));

	println!();

	println!(
		"GET Time per access: {} ({}s)",
		(stats.total_get_time as f64 / stats.num_gets as f64).round(),
		std::char::from_u32(0x03bc).unwrap(),
	);

	println!(
		"SET Time per access: {} ({}s)",
		(stats.total_set_time as f64 / stats.num_sets as f64).round(),
		std::char::from_u32(0x03bc).unwrap(),
	);
}
