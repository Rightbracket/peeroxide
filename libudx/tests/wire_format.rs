#![deny(clippy::all)]

use serde_json::Value;

const FLAG_DATA: u8 = 0x01;
const FLAG_END: u8 = 0x02;
const FLAG_SACK: u8 = 0x04;
const FLAG_MESSAGE: u8 = 0x08;
const FLAG_DESTROY: u8 = 0x10;
const FLAG_HEARTBEAT: u8 = 0x20;

fn load_fixtures() -> Value {
    let fixture_path = format!(
        "{}/tests/fixtures/wire-format-reference.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let data = std::fs::read_to_string(&fixture_path)
        .unwrap_or_else(|e| panic!("failed to read fixture {fixture_path}: {e}"));
    serde_json::from_str(&data).expect("failed to parse fixture JSON")
}

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("invalid hex"))
        .collect()
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

#[test]
fn header_magic_and_version() {
    let fixtures = load_fixtures();
    for fixture in fixtures["fixtures"].as_array().unwrap() {
        let buf = decode_hex(fixture["hex"].as_str().unwrap());
        assert!(buf.len() >= 20, "packet too short: {} bytes", buf.len());
        assert_eq!(buf[0], 0xFF, "magic byte must be 0xFF");
        assert_eq!(buf[1], 1, "version must be 1");
    }
}

#[test]
fn header_data_packet_fields() {
    let fixtures = load_fixtures();
    let data_packet = &fixtures["fixtures"][0];
    let buf = decode_hex(data_packet["hex"].as_str().unwrap());

    assert_eq!(buf[0], 0xFF);
    assert_eq!(buf[1], 1);
    assert_eq!(buf[2], FLAG_DATA);
    assert_eq!(buf[3], 0, "data_offset should be 0 for simple data");

    let remote_id = read_u32_le(&buf, 4);
    assert_eq!(remote_id, 42, "remote_id should be 42 (the REMOTE_ID used in capture)");

    let recv_window = read_u32_le(&buf, 8);
    assert_eq!(recv_window, 4_194_304, "recv_window should be 4MB (DEFAULT_RWND)");

    let seq = read_u32_le(&buf, 12);
    assert_eq!(seq, 0, "first packet seq should be 0");

    let ack = read_u32_le(&buf, 16);
    assert_eq!(ack, 0, "ack should be 0 (no ACKs received yet)");

    assert_eq!(&buf[20..], b"hello", "payload should be 'hello'");
}

#[test]
fn header_end_packet_fields() {
    let fixtures = load_fixtures();
    let end_packet = &fixtures["fixtures"][1];
    let buf = decode_hex(end_packet["hex"].as_str().unwrap());

    assert_eq!(buf.len(), 20, "END packet should be header-only (20 bytes)");
    assert_eq!(buf[0], 0xFF);
    assert_eq!(buf[1], 1);
    assert_eq!(buf[2], FLAG_END);
    assert_eq!(buf[3], 0);

    let remote_id = read_u32_le(&buf, 4);
    assert_eq!(remote_id, 42);

    let seq = read_u32_le(&buf, 12);
    assert_eq!(seq, 1, "END packet should have seq=1 (second packet)");
}

#[test]
fn header_endianness_verification() {
    let fixtures = load_fixtures();
    let data_packet = &fixtures["fixtures"][0];
    let buf = decode_hex(data_packet["hex"].as_str().unwrap());

    // remote_id = 42 = 0x0000002A in LE: bytes 4-7 should be [2A, 00, 00, 00]
    assert_eq!(buf[4], 0x2A);
    assert_eq!(buf[5], 0x00);
    assert_eq!(buf[6], 0x00);
    assert_eq!(buf[7], 0x00);

    // recv_window = 4194304 = 0x00400000 in LE: bytes 8-11 should be [00, 00, 40, 00]
    assert_eq!(buf[8], 0x00);
    assert_eq!(buf[9], 0x00);
    assert_eq!(buf[10], 0x40);
    assert_eq!(buf[11], 0x00);
}

#[test]
fn header_all_flag_constants() {
    assert_eq!(FLAG_DATA, 0b000001);
    assert_eq!(FLAG_END, 0b000010);
    assert_eq!(FLAG_SACK, 0b000100);
    assert_eq!(FLAG_MESSAGE, 0b001000);
    assert_eq!(FLAG_DESTROY, 0b010000);
    assert_eq!(FLAG_HEARTBEAT, 0b100000);

    assert_eq!(FLAG_DATA | FLAG_SACK, 0x05, "DATA+SACK combined");
    assert_eq!(FLAG_DATA | FLAG_END, 0x03, "DATA+END combined");
}

#[test]
fn header_roundtrip_encode_decode() {
    let fixtures = load_fixtures();
    for fixture in fixtures["fixtures"].as_array().unwrap() {
        let hex = fixture["hex"].as_str().unwrap();
        let buf = decode_hex(hex);
        let header = &fixture["header"];

        assert_eq!(buf[0] as u64, header["magic"].as_u64().unwrap());
        assert_eq!(buf[1] as u64, header["version"].as_u64().unwrap());
        assert_eq!(buf[2] as u64, header["typeFlags"].as_u64().unwrap());
        assert_eq!(buf[3] as u64, header["dataOffset"].as_u64().unwrap());
        assert_eq!(read_u32_le(&buf, 4) as u64, header["remoteId"].as_u64().unwrap());
        assert_eq!(read_u32_le(&buf, 8) as u64, header["recvWindow"].as_u64().unwrap());
        assert_eq!(read_u32_le(&buf, 12) as u64, header["seq"].as_u64().unwrap());
        assert_eq!(read_u32_le(&buf, 16) as u64, header["ack"].as_u64().unwrap());
    }
}

#[test]
fn header_data_offset_with_no_sack() {
    let fixtures = load_fixtures();
    for fixture in fixtures["fixtures"].as_array().unwrap() {
        let buf = decode_hex(fixture["hex"].as_str().unwrap());
        let type_flags = buf[2];
        let data_offset = buf[3];

        if type_flags & FLAG_SACK == 0 {
            assert_eq!(data_offset, 0, "data_offset should be 0 when SACK flag is not set");
        }
    }
}

#[test]
fn fixture_packet_lengths() {
    let fixtures = load_fixtures();
    let packets = fixtures["fixtures"].as_array().unwrap();

    assert!(packets.len() >= 2, "expected at least 2 fixture packets");

    let data_pkt = decode_hex(packets[0]["hex"].as_str().unwrap());
    assert_eq!(data_pkt.len(), 25, "DATA('hello') = 20 header + 5 payload");

    let end_pkt = decode_hex(packets[1]["hex"].as_str().unwrap());
    assert_eq!(end_pkt.len(), 20, "END = 20 header only");
}
