use std::ops::AddAssign;

#[derive(Debug, Default, Clone)]
pub struct Stats {
	pub num_gets: u64,
	pub num_sets: u64,

	pub total_get_time: u64,
	pub total_set_time: u64,
}

impl AddAssign for Stats {
	fn add_assign(&mut self, rhs: Self) {
		self.num_gets += rhs.num_gets;
		self.num_sets += rhs.num_sets;

		self.total_get_time += rhs.total_get_time;
		self.total_set_time += rhs.total_set_time;
	}
}
