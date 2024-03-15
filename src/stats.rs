use std::{
	ops::AddAssign,
	time::Duration,
	collections::BTreeMap,
};

#[derive(Debug, Default, Clone)]
pub struct Stats {
	ping_times: BTreeMap<u64, u64>,
	get_times: BTreeMap<u64, u64>,
	set_times: BTreeMap<u64, u64>,

	ping_total_time: u64,
	get_total_time: u64,
	set_total_time: u64,
}

impl Stats {
	pub fn ping(&mut self, duration: Duration) {
		let time = duration.as_micros() as u64;

		self.ping_total_time += time;
		record_time(&mut self.ping_times, time);
	}

	pub fn get(&mut self, duration: Duration) {
		let time = duration.as_micros() as u64;

		self.get_total_time += time;
		record_time(&mut self.get_times, time);
	}

	pub fn set(&mut self, duration: Duration) {
		let time = duration.as_micros() as u64;

		self.set_total_time += time;
		record_time(&mut self.set_times, time);
	}

	pub fn print_ping_stats(&self) {
		let total_pings = self.ping_times.values().sum();

		if total_pings == 0 {
			return;
		}

		println!("\nPING stats");
		print_times(&self.ping_times, total_pings);
	}

	pub fn print_get_stats(&self) {
		let total_gets = self.get_times.values().sum();

		if total_gets == 0 {
			return;
		}

		println!("\nGET stats");
		print_times(&self.get_times, total_gets);
	}

	pub fn print_set_stats(&self) {
		let total_sets = self.set_times.values().sum();

		if total_sets == 0 {
			return;
		}

		println!("\nSET stats");
		print_times(&self.set_times, total_sets);
	}
}

impl AddAssign for Stats {
	fn add_assign(&mut self, rhs: Self) {
		*self = Stats {
			ping_times: merge_times(&self.ping_times, &rhs.ping_times),
			get_times: merge_times(&self.get_times, &rhs.get_times),
			set_times: merge_times(&self.get_times, &rhs.get_times),

			ping_total_time: self.ping_total_time + rhs.ping_total_time,
			get_total_time: self.get_total_time + rhs.get_total_time,
			set_total_time: self.set_total_time + rhs.set_total_time,
		}
	}
}

fn print_times(times: &BTreeMap<u64, u64>, total_count: u64) {
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
	map.extend(times_b);

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
	(time / 10) * 10
}
