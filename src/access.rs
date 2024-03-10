use std::io::{Cursor, Error, ErrorKind};
use byteorder::{LittleEndian, ReadBytesExt};

use kwik::{
	binary_reader::{SizedChunk, Chunk as ReadChunk},
	binary_writer::Chunk as WriteChunk,
};

pub enum Command {
	Get,
	Set,
}

pub struct Access {
	pub timestamp: u64,
	pub command: Command,

	pub key: String,
	pub value: Box<[u8]>,

	pub ttl: Option<u32>,
}

impl SizedChunk for Access {
	fn size() -> usize { 25 }
}

impl ReadChunk for Access {
	fn new(buf: &[u8]) -> Result<Self, Error> {
		let mut rdr = Cursor::new(buf);

		let Ok(timestamp) = rdr.read_u64::<LittleEndian>() else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid access key."
			));
		};

		let command = match rdr.read_u8() {
			Ok(byte) => Command::from_byte(byte)?,

			Err(_) => return Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid access command."
			)),
		};

		let key = match rdr.read_u64::<LittleEndian>() {
			Ok(key) => key.to_string(),

			Err(_) => return Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid access key."
			)),
		};

		let value = match rdr.read_u32::<LittleEndian>() {
			Ok(size) => [0u8].repeat(size as usize).into(),

			Err(_) => return Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid access size."
			)),
		};

		let ttl = match rdr.read_u32::<LittleEndian>() {
			Ok(ttl) => match ttl {
				0 => None,
				ttl => Some(ttl),
			},

			Err(_) => return Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid access ttl."
			)),
		};

		let access = Access {
			timestamp,
			command,

			key,
			value,

			ttl,
		};

		Ok(access)
	}
}

impl WriteChunk for Access {
	fn as_chunk(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
		let Ok(key) = self.key.parse::<u64>() else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid access key."
			));
		};

		let size = self.value.len() as u32;

		buf.extend_from_slice(&self.timestamp.to_le_bytes());
		buf.extend_from_slice(&self.command.as_byte().to_le_bytes());
		buf.extend_from_slice(&key.to_le_bytes());
		buf.extend_from_slice(&size.to_le_bytes());
		buf.extend_from_slice(&self.ttl.unwrap_or(0).to_le_bytes());

		Ok(())
	}
}

impl Command {
	fn from_byte(byte: u8) -> Result<Self, Error> {
		match byte {
			0 => Ok(Command::Get),
			1 => Ok(Command::Set),

			_ => Err(Error::new(
				ErrorKind::InvalidData,
				"Invalid command byte."
			)),
		}
	}

	fn as_byte(&self) -> u8 {
		match self {
			Command::Get => 0,
			Command::Set => 1,
		}
	}
}
