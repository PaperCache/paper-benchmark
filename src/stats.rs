use std::{
	ops::AddAssign,
	time::Duration,
	collections::BTreeMap,
};

use kwik::fmt;

#[derive(Debug, Default, Clone)]
pub struct Stats {
	ping_times: BTreeMap<u64, u64>,
	get_times: BTreeMap<u64, u64>,
	set_times: BTreeMap<u64, u64>,

	ping_total_time: u64,
	get_total_time: u64,
	set_total_time: u64,

	get_total_size: u64,
	set_total_size: u64,
}

impl Stats {
	pub fn store_ping_time(&mut self, duration: Duration) {
		let time = duration.as_micros() as u64;

		self.ping_total_time += time;
		record_time(&mut self.ping_times, time);
	}

	pub fn store_get_time(&mut self, duration: Duration) {
		let time = duration.as_micros() as u64;
		self.get_total_time += time;
		record_time(&mut self.get_times, time);
	}

	pub fn store_get_size(&mut self, size: u64) {
		self.get_total_size += size;
	}

	pub fn store_set_time(&mut self, duration: Duration) {
		let time = duration.as_micros() as u64;
		self.set_total_time += time;
		record_time(&mut self.set_times, time);
	}

	pub fn store_set_size(&mut self, size: u64) {
		self.set_total_size += size;
	}

	pub fn print_ping_stats(&self) {
		let total_pings = self.ping_times.values().sum();

		if total_pings == 0 {
			return;
		}

		println!("\n*** PING stats ***\n");
		print_times(&self.ping_times, total_pings);

		println!(
			"\nAvg latency:\t{} ({}s)",
			(self.ping_total_time as f64 / total_pings as f64).round(),
			std::char::from_u32(0x03bc).unwrap(),
		);

		let ping_rate = total_pings as f64 / (self.ping_total_time / 1_000_000) as f64;
		println!("PINGs/sec:\t{}", fmt::number(ping_rate as u64));
	}

	pub fn print_get_stats(&self) {
		let total_gets = self.get_times.values().sum();

		if total_gets == 0 {
			return;
		}

		println!("\n*** GET stats ***\n");
		print_times(&self.get_times, total_gets);

		println!(
			"\nAvg latency:\t{} ({}s)",
			(self.get_total_time as f64 / total_gets as f64).round(),
			std::char::from_u32(0x03bc).unwrap(),
		);

		let get_rate = total_gets as f64 / (self.get_total_time / 1_000_000) as f64;
		println!("GETs/sec:\t{}", fmt::number(get_rate as u64));

		let avg_size = (self.get_total_size as f64 / total_gets as f64) as u64;
		println!(
			"Avg GET size:\t{} ({} B)",
			fmt::memory(avg_size, Some(2)),
			avg_size,
		);
	}

	pub fn print_set_stats(&self) {
		let total_sets = self.set_times.values().sum();

		if total_sets == 0 {
			return;
		}

		println!("\n*** SET stats ***\n");
		print_times(&self.set_times, total_sets);

		println!(
			"\nAvg latency:\t{} ({}s)",
			(self.set_total_time as f64 / total_sets as f64).round(),
			std::char::from_u32(0x03bc).unwrap(),
		);

		let set_rate = total_sets as f64 / (self.set_total_time / 1_000_000) as f64;
		println!("SETs/sec:\t{}", fmt::number(set_rate as u64));

		let avg_size = (self.set_total_size as f64 / total_sets as f64) as u64;
		println!(
			"Avg SET size:\t{} ({} B)",
			fmt::memory(avg_size, Some(2)),
			avg_size,
		);
	}
}

impl AddAssign for Stats {
	fn add_assign(&mut self, rhs: Self) {
		*self = Stats {
			ping_times: merge_times(&self.ping_times, &rhs.ping_times),
			get_times: merge_times(&self.get_times, &rhs.get_times),
			set_times: merge_times(&self.set_times, &rhs.set_times),

			ping_total_time: self.ping_total_time + rhs.ping_total_time,
			get_total_time: self.get_total_time + rhs.get_total_time,
			set_total_time: self.set_total_time + rhs.set_total_time,

			get_total_size: self.get_total_size + rhs.get_total_size,
			set_total_size: self.set_total_size + rhs.set_total_size,
		}
	}
}

fn print_times(times: &BTreeMap<u64, u64>, total_count: u64) {
	println!("Latency distribution:");

	let mut count_sum = 0;

	for (time, count) in times.iter() {
		count_sum += count;

		let ratio = count_sum as f64 / total_count as f64;

		println!(
			"<= {} {}s\t{:.2} %",
			time,
			std::char::from_u32(0x03bc).unwrap(),
			ratio * 100.0,
		);

		if ratio >= 0.99 {
			break;
		}
	}

	if count_sum != total_count {
		if let Some((time, _)) = times.last_key_value() {
			println!(
				"<= {} {}s\t{:.2} %",
				time,
				std::char::from_u32(0x03bc).unwrap(),
				100,
			);
		}
	}
}

fn merge_times(
	times_a: &BTreeMap<u64, u64>,
	times_b: &BTreeMap<u64, u64>,
) -> BTreeMap<u64, u64> {
	let mut map = BTreeMap::<u64, u64>::new();
	map.extend(times_a);

	for (time_b, count_b) in times_b {
		match map.get_mut(time_b) {
			Some(count) => *count += count_b,

			None => {
				map.insert(*time_b, *count_b);
			},
		}
	}

	map
}

fn record_time(times: &mut BTreeMap<u64, u64>, time: u64) {
	let time = get_rounded_time(time);

	match times.get_mut(&time) {
		Some(count) => *count += 1,

		None => {
			times.insert(time, 1);
		},
	}
}

fn get_rounded_time(time: u64) -> u64 {
	(time as f64 / 10.0).round() as u64 * 10
}
