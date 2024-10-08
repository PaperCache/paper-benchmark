use std::{
	io,
	ops::AddAssign,
	time::Duration,
};

use statrs::statistics::{Data, OrderStatistics};

use kwik::{
	fmt,
	table::{
		Table,
		Row,
		Align,
		Style,
	},
};

type LatencyData = Data<Vec<f64>>;

#[derive(Debug, Default, Clone)]
pub struct Stats {
	ping_latencies: Vec<Duration>,
	get_latencies: Vec<Duration>,
	set_latencies: Vec<Duration>,

	get_total_size: u64,
	set_total_size: u64,
}

impl Stats {
	pub fn store_ping_time(&mut self, duration: Duration) {
		self.ping_latencies.push(duration);
	}

	pub fn store_get_time(&mut self, duration: Duration) {
		self.get_latencies.push(duration);
	}

	pub fn store_get_size(&mut self, size: u64) {
		self.get_total_size += size;
	}

	pub fn store_set_time(&mut self, duration: Duration) {
		self.set_latencies.push(duration);
	}

	pub fn store_set_size(&mut self, size: u64) {
		self.set_total_size += size;
	}

	pub fn print_ping_stats(&self) {
		print_stats("PING", &self.ping_latencies);
	}

	pub fn print_get_stats(&self) {
		print_stats("GET", &self.get_latencies);

		if !self.get_latencies.is_empty() {
			let avg_size = (self.get_total_size as f64 / self.get_latencies.len() as f64) as u64;

			println!(
				"Avg GET size:\t{} ({} B)",
				fmt::memory(avg_size, Some(2)),
				avg_size,
			);
		}
	}

	pub fn print_set_stats(&self) {
		print_stats("SET", &self.set_latencies);

		if !self.set_latencies.is_empty() {
			let avg_size = (self.set_total_size as f64 / self.set_latencies.len() as f64) as u64;

			println!(
				"Avg SET size:\t{} ({} B)",
				fmt::memory(avg_size, Some(2)),
				avg_size,
			);
		}
	}
}

impl AddAssign for Stats {
	fn add_assign(&mut self, rhs: Self) {
		*self = Stats {
			ping_latencies: merge_times(&self.ping_latencies, &rhs.ping_latencies),
			get_latencies: merge_times(&self.get_latencies, &rhs.get_latencies),
			set_latencies: merge_times(&self.set_latencies, &rhs.set_latencies),

			get_total_size: self.get_total_size + rhs.get_total_size,
			set_total_size: self.set_total_size + rhs.set_total_size,
		}
	}
}

fn print_stats(label: &'static str, durations: &[Duration]) {
	let latencies = durations
		.iter()
		.map(|duration| duration.as_micros() as f64)
		.collect::<Vec<_>>();

	let mut data = Data::new(latencies);

	if data.is_empty() {
		return;
	}

	println!("\n*** {label} stats ***\n");

	print_dist(&mut data);
	print_simple_stats(label, &data);
}

fn print_dist(data: &mut LatencyData) {
	let mut table = Table::default();

	let quantiles: &[f64] = &[
		0.5,
		0.75,
		0.90,
		0.95,
		0.99,
		0.999,
		0.9999,
		0.99999,
		1.0,
	];

	let mut header = Row::default();
	let mut row = Row::default();

	for quantile in quantiles {
		let multiplier = match quantile {
			0.999 => 1000.0,
			0.9999 => 10000.0,
			0.99999 => 100000.0,
			_ => 100.0,
		};

		let label = format!("p{}", (quantile * multiplier).round());
		let value = format!("{:.0}us", data.quantile(*quantile));

		header = header.push(label, Align::Center, Style::Bold);
		row = row.push(value, Align::Center, Style::Normal);
	}

	table.set_header(header);
	table.add_row(row);

	let mut stdout = io::stdout().lock();
	table.print(&mut stdout);
}

fn print_simple_stats(label: &'static str, data: &LatencyData) {
	let total_time = data
		.iter()
		.sum::<f64>();

	println!(
		"\nAvg latency:\t{}us",
		(total_time / data.len() as f64).round(),
	);

	let rate = data.len() as f64 / (total_time / 1_000_000.0);

	println!(
		"{label}s/sec:\t{}",
		fmt::number(rate as u64),
	);
}

fn merge_times(times_a: &[Duration], times_b: &[Duration]) -> Vec<Duration> {
	let mut times = Vec::<Duration>::new();

	times.extend_from_slice(times_a);
	times.extend_from_slice(times_b);

	times
}
