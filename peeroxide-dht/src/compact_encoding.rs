#![allow(missing_docs)]
#![allow(dead_code)]

use thiserror::Error;

/// Errors returned by compact encoding operations
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum EncodingError {
    #[error("out of bounds: need {need} bytes, have {have}")]
    /// The buffer did not contain enough bytes
    OutOfBounds {
        /// The number of bytes needed
        need: usize,
        /// The number of bytes available
        have: usize,
    },
    #[error("incorrect buffer size: expected {expected}, got {got}")]
    /// The input buffer did not match the expected size
    IncorrectBufferSize {
        /// The expected buffer length
        expected: usize,
        /// The actual buffer length
        got: usize,
    },
    #[error("array too large: {0} elements (max 1048576)")]
    /// The array length exceeds the supported maximum
    ArrayTooLarge(#[doc = "The number of elements requested"] usize),
    #[error("invalid IP family: {0}")]
    /// The encoded IP family tag was not recognized
    InvalidIpFamily(#[doc = "The invalid family tag value"] u8),
    #[error("invalid IPv4 address: {0}")]
    /// The provided IPv4 address was invalid
    InvalidIpv4(#[doc = "The invalid IPv4 address string"] String),
    #[error("invalid IPv6 address: {0}")]
    /// The provided IPv6 address was invalid
    InvalidIpv6(#[doc = "The invalid IPv6 address string"] String),
}

/// Result type used by compact encoding operations
pub type Result<T> = std::result::Result<T, EncodingError>;

/// Encoding cursor and backing buffer state
#[derive(Debug, Clone)]
pub struct State {
    /// The current read or write position
    pub start: usize,
    /// The allocated end position for pre-encoded data
    pub end: usize,
    /// The backing byte buffer
    pub buffer: Vec<u8>,
}

impl State {
    /// Creates a new empty state
    pub fn new() -> Self {
        Self {
            start: 0,
            end: 0,
            buffer: Vec::new(),
        }
    }

    /// Creates a state from an existing buffer
    pub fn from_buffer(buffer: &[u8]) -> Self {
        Self {
            start: 0,
            end: buffer.len(),
            buffer: buffer.to_vec(),
        }
    }

    /// Allocates a buffer based on pre-encoded size
    pub fn alloc(&mut self) {
        if self.buffer.len() < self.end {
            self.buffer.resize(self.end, 0);
        }
    }

    fn remaining(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    fn check_remaining(&self, n: usize) -> Result<()> {
        if self.remaining() < n {
            return Err(EncodingError::OutOfBounds {
                need: n,
                have: self.remaining(),
            });
        }
        Ok(())
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

/// Pre-encodes a uint8 value, advancing the state cursor
pub fn preencode_uint8(state: &mut State, _val: u8) {
    state.end += 1;
}

/// Encodes a uint8 value into the state buffer
pub fn encode_uint8(state: &mut State, val: u8) {
    state.buffer[state.start] = val;
    state.start += 1;
}

/// Decodes a uint8 value from the state buffer
pub fn decode_uint8(state: &mut State) -> Result<u8> {
    state.check_remaining(1)?;
    let val = state.buffer[state.start];
    state.start += 1;
    Ok(val)
}

/// Pre-encodes a uint16 value, advancing the state cursor
pub fn preencode_uint16(state: &mut State, _val: u16) {
    state.end += 2;
}

/// Encodes a uint16 value into the state buffer
pub fn encode_uint16(state: &mut State, val: u16) {
    let bytes = val.to_le_bytes();
    state.buffer[state.start] = bytes[0];
    state.buffer[state.start + 1] = bytes[1];
    state.start += 2;
}

/// Decodes a uint16 value from the state buffer
pub fn decode_uint16(state: &mut State) -> Result<u16> {
    state.check_remaining(2)?;
    let val = u16::from_le_bytes([state.buffer[state.start], state.buffer[state.start + 1]]);
    state.start += 2;
    Ok(val)
}

/// Pre-encodes a uint24 value, advancing the state cursor
pub fn preencode_uint24(state: &mut State, _val: u32) {
    state.end += 3;
}

/// Encodes a uint24 value into the state buffer
pub fn encode_uint24(state: &mut State, val: u32) {
    state.buffer[state.start] = val as u8;
    state.buffer[state.start + 1] = (val >> 8) as u8;
    state.buffer[state.start + 2] = (val >> 16) as u8;
    state.start += 3;
}

/// Decodes a uint24 value from the state buffer
pub fn decode_uint24(state: &mut State) -> Result<u32> {
    state.check_remaining(3)?;
    let val = state.buffer[state.start] as u32
        | (state.buffer[state.start + 1] as u32) << 8
        | (state.buffer[state.start + 2] as u32) << 16;
    state.start += 3;
    Ok(val)
}

/// Pre-encodes a uint32 value, advancing the state cursor
pub fn preencode_uint32(state: &mut State, _val: u32) {
    state.end += 4;
}

/// Encodes a uint32 value into the state buffer
pub fn encode_uint32(state: &mut State, val: u32) {
    let bytes = val.to_le_bytes();
    state.buffer[state.start..state.start + 4].copy_from_slice(&bytes);
    state.start += 4;
}

/// Decodes a uint32 value from the state buffer
pub fn decode_uint32(state: &mut State) -> Result<u32> {
    state.check_remaining(4)?;
    let val = u32::from_le_bytes([
        state.buffer[state.start],
        state.buffer[state.start + 1],
        state.buffer[state.start + 2],
        state.buffer[state.start + 3],
    ]);
    state.start += 4;
    Ok(val)
}

/// Pre-encodes a uint40 value, advancing the state cursor
pub fn preencode_uint40(state: &mut State, _val: u64) {
    state.end += 5;
}

/// Encodes a uint40 value into the state buffer
pub fn encode_uint40(state: &mut State, val: u64) {
    encode_uint8(state, val as u8);
    encode_uint32(state, (val >> 8) as u32);
}

/// Decodes a uint40 value from the state buffer
pub fn decode_uint40(state: &mut State) -> Result<u64> {
    state.check_remaining(5)?;
    let lo = decode_uint8(state)? as u64;
    let hi = decode_uint32(state)? as u64;
    Ok(lo | (hi << 8))
}

/// Pre-encodes a uint48 value, advancing the state cursor
pub fn preencode_uint48(state: &mut State, _val: u64) {
    state.end += 6;
}

/// Encodes a uint48 value into the state buffer
pub fn encode_uint48(state: &mut State, val: u64) {
    encode_uint16(state, val as u16);
    encode_uint32(state, (val >> 16) as u32);
}

/// Decodes a uint48 value from the state buffer
pub fn decode_uint48(state: &mut State) -> Result<u64> {
    state.check_remaining(6)?;
    let lo = decode_uint16(state)? as u64;
    let hi = decode_uint32(state)? as u64;
    Ok(lo | (hi << 16))
}

/// Pre-encodes a uint56 value, advancing the state cursor
pub fn preencode_uint56(state: &mut State, _val: u64) {
    state.end += 7;
}

/// Encodes a uint56 value into the state buffer
pub fn encode_uint56(state: &mut State, val: u64) {
    encode_uint24(state, val as u32 & 0xFFFFFF);
    encode_uint32(state, (val >> 24) as u32);
}

/// Decodes a uint56 value from the state buffer
pub fn decode_uint56(state: &mut State) -> Result<u64> {
    state.check_remaining(7)?;
    let lo = decode_uint24(state)? as u64;
    let hi = decode_uint32(state)? as u64;
    Ok(lo | (hi << 24))
}

/// Pre-encodes a uint64 value, advancing the state cursor
pub fn preencode_uint64(state: &mut State, _val: u64) {
    state.end += 8;
}

/// Encodes a uint64 value into the state buffer
pub fn encode_uint64(state: &mut State, val: u64) {
    let lo = val as u32;
    let hi = (val >> 32) as u32;
    encode_uint32(state, lo);
    encode_uint32(state, hi);
}

/// Decodes a uint64 value from the state buffer
pub fn decode_uint64(state: &mut State) -> Result<u64> {
    state.check_remaining(8)?;
    let lo = decode_uint32(state)? as u64;
    let hi = decode_uint32(state)? as u64;
    Ok(lo | (hi << 32))
}

/// Pre-encodes a uint value, advancing the state cursor
pub fn preencode_uint(state: &mut State, n: u64) {
    if n <= 0xfc {
        state.end += 1;
    } else if n <= 0xffff {
        state.end += 3;
    } else if n <= 0xffffffff {
        state.end += 5;
    } else {
        state.end += 9;
    }
}

/// Encodes a uint value into the state buffer
pub fn encode_uint(state: &mut State, n: u64) {
    if n <= 0xfc {
        encode_uint8(state, n as u8);
    } else if n <= 0xffff {
        state.buffer[state.start] = 0xfd;
        state.start += 1;
        encode_uint16(state, n as u16);
    } else if n <= 0xffffffff {
        state.buffer[state.start] = 0xfe;
        state.start += 1;
        encode_uint32(state, n as u32);
    } else {
        state.buffer[state.start] = 0xff;
        state.start += 1;
        encode_uint64(state, n);
    }
}

/// Decodes a uint value from the state buffer
pub fn decode_uint(state: &mut State) -> Result<u64> {
    let a = decode_uint8(state)?;
    if a <= 0xfc {
        return Ok(a as u64);
    }
    if a == 0xfd {
        return Ok(decode_uint16(state)? as u64);
    }
    if a == 0xfe {
        return Ok(decode_uint32(state)? as u64);
    }
    decode_uint64(state)
}

fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Pre-encodes an int value, advancing the state cursor
pub fn preencode_int(state: &mut State, n: i64) {
    preencode_uint(state, zigzag_encode(n));
}

/// Encodes an int value into the state buffer
pub fn encode_int(state: &mut State, n: i64) {
    encode_uint(state, zigzag_encode(n));
}

/// Decodes an int value from the state buffer
pub fn decode_int(state: &mut State) -> Result<i64> {
    Ok(zigzag_decode(decode_uint(state)?))
}

/// Pre-encodes an int8 value, advancing the state cursor
pub fn preencode_int8(state: &mut State, _val: i8) {
    state.end += 1;
}

/// Encodes an int8 value into the state buffer
pub fn encode_int8(state: &mut State, val: i8) {
    let z = zigzag_encode(val as i64);
    encode_uint8(state, z as u8);
}

/// Decodes an int8 value from the state buffer
pub fn decode_int8(state: &mut State) -> Result<i8> {
    Ok(zigzag_decode(decode_uint8(state)? as u64) as i8)
}

/// Pre-encodes an int16 value, advancing the state cursor
pub fn preencode_int16(state: &mut State, _val: i16) {
    state.end += 2;
}

/// Encodes an int16 value into the state buffer
pub fn encode_int16(state: &mut State, val: i16) {
    let z = zigzag_encode(val as i64);
    encode_uint16(state, z as u16);
}

/// Decodes an int16 value from the state buffer
pub fn decode_int16(state: &mut State) -> Result<i16> {
    Ok(zigzag_decode(decode_uint16(state)? as u64) as i16)
}

/// Pre-encodes an int32 value, advancing the state cursor
pub fn preencode_int32(state: &mut State, _val: i32) {
    state.end += 4;
}

/// Encodes an int32 value into the state buffer
pub fn encode_int32(state: &mut State, val: i32) {
    let z = zigzag_encode(val as i64);
    encode_uint32(state, z as u32);
}

/// Decodes an int32 value from the state buffer
pub fn decode_int32(state: &mut State) -> Result<i32> {
    Ok(zigzag_decode(decode_uint32(state)? as u64) as i32)
}

/// Pre-encodes an int64 value, advancing the state cursor
pub fn preencode_int64(state: &mut State, _val: i64) {
    state.end += 8;
}

/// Encodes an int64 value into the state buffer
pub fn encode_int64(state: &mut State, val: i64) {
    let z = zigzag_encode(val);
    encode_uint64(state, z);
}

/// Decodes an int64 value from the state buffer
pub fn decode_int64(state: &mut State) -> Result<i64> {
    Ok(zigzag_decode(decode_uint64(state)?))
}

/// Pre-encodes a float32 value, advancing the state cursor
pub fn preencode_float32(state: &mut State, _val: f32) {
    state.end += 4;
}

/// Encodes a float32 value into the state buffer
pub fn encode_float32(state: &mut State, val: f32) {
    let bytes = val.to_le_bytes();
    state.buffer[state.start..state.start + 4].copy_from_slice(&bytes);
    state.start += 4;
}

/// Decodes a float32 value from the state buffer
pub fn decode_float32(state: &mut State) -> Result<f32> {
    state.check_remaining(4)?;
    let val = f32::from_le_bytes([
        state.buffer[state.start],
        state.buffer[state.start + 1],
        state.buffer[state.start + 2],
        state.buffer[state.start + 3],
    ]);
    state.start += 4;
    Ok(val)
}

/// Pre-encodes a float64 value, advancing the state cursor
pub fn preencode_float64(state: &mut State, _val: f64) {
    state.end += 8;
}

/// Encodes a float64 value into the state buffer
pub fn encode_float64(state: &mut State, val: f64) {
    let bytes = val.to_le_bytes();
    state.buffer[state.start..state.start + 8].copy_from_slice(&bytes);
    state.start += 8;
}

/// Decodes a float64 value from the state buffer
pub fn decode_float64(state: &mut State) -> Result<f64> {
    state.check_remaining(8)?;
    let val = f64::from_le_bytes([
        state.buffer[state.start],
        state.buffer[state.start + 1],
        state.buffer[state.start + 2],
        state.buffer[state.start + 3],
        state.buffer[state.start + 4],
        state.buffer[state.start + 5],
        state.buffer[state.start + 6],
        state.buffer[state.start + 7],
    ]);
    state.start += 8;
    Ok(val)
}

/// Pre-encodes a bool value, advancing the state cursor
pub fn preencode_bool(state: &mut State, _val: bool) {
    state.end += 1;
}

/// Encodes a bool value into the state buffer
pub fn encode_bool(state: &mut State, val: bool) {
    encode_uint8(state, if val { 1 } else { 0 });
}

/// Decodes a bool value from the state buffer
pub fn decode_bool(state: &mut State) -> Result<bool> {
    Ok(decode_uint8(state)? != 0)
}

/// Pre-encodes an optional buffer, advancing the state cursor
pub fn preencode_buffer(state: &mut State, buf: Option<&[u8]>) {
    match buf {
        Some(b) => {
            preencode_uint(state, b.len() as u64);
            state.end += b.len();
        }
        None => state.end += 1,
    }
}

/// Encodes an optional buffer into the state buffer
pub fn encode_buffer(state: &mut State, buf: Option<&[u8]>) {
    match buf {
        Some(b) => {
            encode_uint(state, b.len() as u64);
            state.buffer[state.start..state.start + b.len()].copy_from_slice(b);
            state.start += b.len();
        }
        None => {
            state.buffer[state.start] = 0;
            state.start += 1;
        }
    }
}

/// Decodes an optional buffer from the state buffer
pub fn decode_buffer(state: &mut State) -> Result<Option<Vec<u8>>> {
    let len = decode_uint(state)? as usize;
    if len == 0 {
        return Ok(None);
    }
    state.check_remaining(len)?;
    let val = state.buffer[state.start..state.start + len].to_vec();
    state.start += len;
    Ok(Some(val))
}

/// Pre-encodes a string value, advancing the state cursor
pub fn preencode_string(state: &mut State, s: &str) {
    let len = s.len();
    preencode_uint(state, len as u64);
    state.end += len;
}

/// Encodes a string value into the state buffer
pub fn encode_string(state: &mut State, s: &str) {
    let bytes = s.as_bytes();
    encode_uint(state, bytes.len() as u64);
    state.buffer[state.start..state.start + bytes.len()].copy_from_slice(bytes);
    state.start += bytes.len();
}

/// Decodes a string value from the state buffer
pub fn decode_string(state: &mut State) -> Result<String> {
    let len = decode_uint(state)? as usize;
    state.check_remaining(len)?;
    let val = String::from_utf8_lossy(&state.buffer[state.start..state.start + len]).into_owned();
    state.start += len;
    Ok(val)
}

/// Pre-encodes a fixed-length buffer, advancing the state cursor
pub fn preencode_fixed(state: &mut State, n: usize, buf: &[u8]) -> Result<()> {
    if buf.len() != n {
        return Err(EncodingError::IncorrectBufferSize {
            expected: n,
            got: buf.len(),
        });
    }
    state.end += n;
    Ok(())
}

/// Encodes a fixed-length buffer into the state buffer
pub fn encode_fixed(state: &mut State, buf: &[u8]) {
    state.buffer[state.start..state.start + buf.len()].copy_from_slice(buf);
    state.start += buf.len();
}

/// Decodes a fixed-length buffer from the state buffer
pub fn decode_fixed(state: &mut State, n: usize) -> Result<Vec<u8>> {
    state.check_remaining(n)?;
    let val = state.buffer[state.start..state.start + n].to_vec();
    state.start += n;
    Ok(val)
}

/// Pre-encodes a 32-byte fixed buffer, advancing the state cursor
pub fn preencode_fixed32(state: &mut State, buf: &[u8; 32]) -> Result<()> {
    preencode_fixed(state, 32, buf)
}

/// Encodes a 32-byte fixed buffer into the state buffer
pub fn encode_fixed32(state: &mut State, buf: &[u8; 32]) {
    encode_fixed(state, buf);
}

/// Decodes a 32-byte fixed buffer from the state buffer
pub fn decode_fixed32(state: &mut State) -> Result<[u8; 32]> {
    state.check_remaining(32)?;
    let mut val = [0u8; 32];
    val.copy_from_slice(&state.buffer[state.start..state.start + 32]);
    state.start += 32;
    Ok(val)
}

/// Pre-encodes a 64-byte fixed buffer, advancing the state cursor
pub fn preencode_fixed64(state: &mut State, buf: &[u8; 64]) -> Result<()> {
    preencode_fixed(state, 64, buf)
}

/// Encodes a 64-byte fixed buffer into the state buffer
pub fn encode_fixed64(state: &mut State, buf: &[u8; 64]) {
    encode_fixed(state, buf);
}

/// Decodes a 64-byte fixed buffer from the state buffer
pub fn decode_fixed64(state: &mut State) -> Result<[u8; 64]> {
    state.check_remaining(64)?;
    let mut val = [0u8; 64];
    val.copy_from_slice(&state.buffer[state.start..state.start + 64]);
    state.start += 64;
    Ok(val)
}

/// Pre-encodes an IPv4 address, advancing the state cursor
pub fn preencode_ipv4(state: &mut State, _addr: &str) {
    state.end += 4;
}

/// Encodes an IPv4 address into the state buffer
pub fn encode_ipv4(state: &mut State, addr: &str) -> Result<()> {
    let parts: Vec<&str> = addr.split('.').collect();
    if parts.len() != 4 {
        return Err(EncodingError::InvalidIpv4(addr.to_string()));
    }
    for part in &parts {
        let byte: u8 = part
            .parse()
            .map_err(|_| EncodingError::InvalidIpv4(addr.to_string()))?;
        state.buffer[state.start] = byte;
        state.start += 1;
    }
    Ok(())
}

/// Decodes an IPv4 address from the state buffer
pub fn decode_ipv4(state: &mut State) -> Result<String> {
    state.check_remaining(4)?;
    let addr = format!(
        "{}.{}.{}.{}",
        state.buffer[state.start],
        state.buffer[state.start + 1],
        state.buffer[state.start + 2],
        state.buffer[state.start + 3]
    );
    state.start += 4;
    Ok(addr)
}

/// Pre-encodes an IPv6 address, advancing the state cursor
pub fn preencode_ipv6(state: &mut State, _addr: &str) {
    state.end += 16;
}

/// Encodes an IPv6 address into the state buffer
pub fn encode_ipv6(state: &mut State, addr: &str) -> Result<()> {
    let parsed: std::net::Ipv6Addr = addr
        .parse()
        .map_err(|_| EncodingError::InvalidIpv6(addr.to_string()))?;
    let octets = parsed.octets();
    state.buffer[state.start..state.start + 16].copy_from_slice(&octets);
    state.start += 16;
    Ok(())
}

/// Decodes an IPv6 address from the state buffer
pub fn decode_ipv6(state: &mut State) -> Result<String> {
    state.check_remaining(16)?;
    let mut octets = [0u8; 16];
    octets.copy_from_slice(&state.buffer[state.start..state.start + 16]);
    state.start += 16;
    let addr = std::net::Ipv6Addr::from(octets);
    Ok(addr.to_string())
}

const IP_FAMILY_V4: u8 = 4;
const IP_FAMILY_V6: u8 = 6;

/// Pre-encodes an IP address, advancing the state cursor
pub fn preencode_ip(state: &mut State, addr: &str) {
    if addr.contains(':') {
        preencode_uint8(state, IP_FAMILY_V6);
        preencode_ipv6(state, addr);
    } else {
        preencode_uint8(state, IP_FAMILY_V4);
        preencode_ipv4(state, addr);
    }
}

/// Encodes an IP address into the state buffer
pub fn encode_ip(state: &mut State, addr: &str) -> Result<()> {
    if addr.contains(':') {
        encode_uint8(state, IP_FAMILY_V6);
        encode_ipv6(state, addr)
    } else {
        encode_uint8(state, IP_FAMILY_V4);
        encode_ipv4(state, addr)
    }
}

/// Decodes an IP address from the state buffer
pub fn decode_ip(state: &mut State) -> Result<String> {
    let family = decode_uint8(state)?;
    match family {
        IP_FAMILY_V4 => decode_ipv4(state),
        IP_FAMILY_V6 => decode_ipv6(state),
        _ => Err(EncodingError::InvalidIpFamily(family)),
    }
}

/// Pre-encodes an IPv4 address and port, advancing the state cursor
pub fn preencode_ipv4_address(state: &mut State, addr: &str, _port: u16) {
    preencode_ipv4(state, addr);
    state.end += 2;
}

/// Encodes an IPv4 address and port into the state buffer
pub fn encode_ipv4_address(state: &mut State, addr: &str, port: u16) -> Result<()> {
    encode_ipv4(state, addr)?;
    encode_uint16(state, port);
    Ok(())
}

/// Decodes an IPv4 address and port from the state buffer
pub fn decode_ipv4_address(state: &mut State) -> Result<(String, u16)> {
    let addr = decode_ipv4(state)?;
    let port = decode_uint16(state)?;
    Ok((addr, port))
}

/// Pre-encodes an IPv6 address and port, advancing the state cursor
pub fn preencode_ipv6_address(state: &mut State, addr: &str, _port: u16) {
    preencode_ipv6(state, addr);
    state.end += 2;
}

/// Encodes an IPv6 address and port into the state buffer
pub fn encode_ipv6_address(state: &mut State, addr: &str, port: u16) -> Result<()> {
    encode_ipv6(state, addr)?;
    encode_uint16(state, port);
    Ok(())
}

/// Decodes an IPv6 address and port from the state buffer
pub fn decode_ipv6_address(state: &mut State) -> Result<(String, u16)> {
    let addr = decode_ipv6(state)?;
    let port = decode_uint16(state)?;
    Ok((addr, port))
}

const MAX_ARRAY_LENGTH: usize = 0x100000;

/// Pre-encodes a uint array, advancing the state cursor
pub fn preencode_uint_array(state: &mut State, arr: &[u64]) {
    preencode_uint(state, arr.len() as u64);
    for &val in arr {
        preencode_uint(state, val);
    }
}

/// Encodes a uint array into the state buffer
pub fn encode_uint_array(state: &mut State, arr: &[u64]) {
    encode_uint(state, arr.len() as u64);
    for &val in arr {
        encode_uint(state, val);
    }
}

/// Decodes a uint array from the state buffer
pub fn decode_uint_array(state: &mut State) -> Result<Vec<u64>> {
    let len = decode_uint(state)? as usize;
    if len > MAX_ARRAY_LENGTH {
        return Err(EncodingError::ArrayTooLarge(len));
    }
    let mut arr = Vec::with_capacity(len);
    for _ in 0..len {
        arr.push(decode_uint(state)?);
    }
    Ok(arr)
}

/// Pre-encodes a buffer array, advancing the state cursor
pub fn preencode_buffer_array(state: &mut State, arr: &[Option<&[u8]>]) {
    preencode_uint(state, arr.len() as u64);
    for buf in arr {
        preencode_buffer(state, *buf);
    }
}

/// Encodes a buffer array into the state buffer
pub fn encode_buffer_array(state: &mut State, arr: &[Option<&[u8]>]) {
    encode_uint(state, arr.len() as u64);
    for buf in arr {
        encode_buffer(state, *buf);
    }
}

/// Decodes a buffer array from the state buffer
pub fn decode_buffer_array(state: &mut State) -> Result<Vec<Option<Vec<u8>>>> {
    let len = decode_uint(state)? as usize;
    if len > MAX_ARRAY_LENGTH {
        return Err(EncodingError::ArrayTooLarge(len));
    }
    let mut arr = Vec::with_capacity(len);
    for _ in 0..len {
        arr.push(decode_buffer(state)?);
    }
    Ok(arr)
}

/// Pre-encodes a string array, advancing the state cursor
pub fn preencode_string_array(state: &mut State, arr: &[&str]) {
    preencode_uint(state, arr.len() as u64);
    for s in arr {
        preencode_string(state, s);
    }
}

/// Encodes a string array into the state buffer
pub fn encode_string_array(state: &mut State, arr: &[&str]) {
    encode_uint(state, arr.len() as u64);
    for s in arr {
        encode_string(state, s);
    }
}

/// Decodes a string array from the state buffer
pub fn decode_string_array(state: &mut State) -> Result<Vec<String>> {
    let len = decode_uint(state)? as usize;
    if len > MAX_ARRAY_LENGTH {
        return Err(EncodingError::ArrayTooLarge(len));
    }
    let mut arr = Vec::with_capacity(len);
    for _ in 0..len {
        arr.push(decode_string(state)?);
    }
    Ok(arr)
}

#[cfg(test)]
mod tests;
