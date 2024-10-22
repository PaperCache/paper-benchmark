use std::io::{self, Cursor};
use byteorder::{LittleEndian, ReadBytesExt};

use kwik::file::binary::{
	SizedChunk,
	ReadChunk,
	WriteChunk,
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
	fn from_chunk(buf: &[u8]) -> io::Result<Self> {
		let mut rdr = Cursor::new(buf);

		let timestamp = rdr.read_u64::<LittleEndian>()?;

		let command_byte = rdr.read_u8()?;
		let command = Command::from_byte(command_byte)?;

		let key = rdr
			.read_u64::<LittleEndian>()?
			.to_string();

		let value_size = rdr.read_u32::<LittleEndian>()?;
		let value = [0u8].repeat(value_size as usize).into();

		let ttl = match rdr.read_u32::<LittleEndian>()? {
			0 => None,
			ttl => Some(ttl),
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
	fn as_chunk(&self, buf: &mut Vec<u8>) -> io::Result<()> {
		let key = self.key
			.parse::<u64>()
			.expect("Invalid access key.");

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
	fn from_byte(byte: u8) -> io::Result<Self> {
		match byte {
			0 => Ok(Command::Get),
			1 => Ok(Command::Set),

			_ => Err(io::Error::new(
				io::ErrorKind::InvalidData,
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
