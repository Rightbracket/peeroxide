use super::*;

#[allow(dead_code)]
fn encode_value(f: impl Fn(&mut State)) -> Vec<u8> {
    let mut state = State::new();
    let mut pre = State::new();
    f(&mut pre);
    state.end = pre.end;
    state.alloc();
    f(&mut state);
    state.buffer
}

fn encode_value_with_pre(
    pre_fn: impl FnOnce(&mut State),
    enc_fn: impl FnOnce(&mut State),
) -> Vec<u8> {
    let mut state = State::new();
    pre_fn(&mut state);
    state.alloc();
    enc_fn(&mut state);
    state.buffer
}

#[test]
fn uint8_roundtrip() {
    for val in [0u8, 1, 127, 128, 255] {
        let buf = encode_value_with_pre(
            |s| preencode_uint8(s, val),
            |s| encode_uint8(s, val),
        );
        assert_eq!(buf, vec![val]);
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_uint8(&mut state).unwrap(), val);
    }
}

#[test]
fn uint16_little_endian() {
    let buf = encode_value_with_pre(
        |s| preencode_uint16(s, 0x0102),
        |s| encode_uint16(s, 0x0102),
    );
    assert_eq!(buf, vec![0x02, 0x01]);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_uint16(&mut state).unwrap(), 0x0102);
}

#[test]
fn uint24_little_endian() {
    let buf = encode_value_with_pre(
        |s| preencode_uint24(s, 0x010203),
        |s| encode_uint24(s, 0x010203),
    );
    assert_eq!(buf, vec![0x03, 0x02, 0x01]);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_uint24(&mut state).unwrap(), 0x010203);
}

#[test]
fn uint32_little_endian() {
    let buf = encode_value_with_pre(
        |s| preencode_uint32(s, 0x01020304),
        |s| encode_uint32(s, 0x01020304),
    );
    assert_eq!(buf, vec![0x04, 0x03, 0x02, 0x01]);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_uint32(&mut state).unwrap(), 0x01020304);
}

#[test]
fn uint64_little_endian() {
    let buf = encode_value_with_pre(
        |s| preencode_uint64(s, 0x0102030405060708),
        |s| encode_uint64(s, 0x0102030405060708),
    );
    assert_eq!(buf, vec![0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_uint64(&mut state).unwrap(), 0x0102030405060708);
}

#[test]
fn uint_varint_1_byte() {
    for val in [0u64, 1, 100, 252] {
        let buf = encode_value_with_pre(
            |s| preencode_uint(s, val),
            |s| encode_uint(s, val),
        );
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], val as u8);
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_uint(&mut state).unwrap(), val);
    }
}

#[test]
fn uint_varint_3_byte() {
    for val in [253u64, 1000, 65535] {
        let buf = encode_value_with_pre(
            |s| preencode_uint(s, val),
            |s| encode_uint(s, val),
        );
        assert_eq!(buf.len(), 3);
        assert_eq!(buf[0], 0xfd);
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_uint(&mut state).unwrap(), val);
    }
}

#[test]
fn uint_varint_5_byte() {
    for val in [65536u64, 100_000, 0xffffffff] {
        let buf = encode_value_with_pre(
            |s| preencode_uint(s, val),
            |s| encode_uint(s, val),
        );
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0xfe);
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_uint(&mut state).unwrap(), val);
    }
}

#[test]
fn uint_varint_9_byte() {
    let val = 0x100000000u64;
    let buf = encode_value_with_pre(
        |s| preencode_uint(s, val),
        |s| encode_uint(s, val),
    );
    assert_eq!(buf.len(), 9);
    assert_eq!(buf[0], 0xff);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_uint(&mut state).unwrap(), val);
}

#[test]
fn zigzag_encoding() {
    assert_eq!(zigzag_encode(0), 0);
    assert_eq!(zigzag_encode(-1), 1);
    assert_eq!(zigzag_encode(1), 2);
    assert_eq!(zigzag_encode(-2), 3);
    assert_eq!(zigzag_encode(2), 4);

    assert_eq!(zigzag_decode(0), 0);
    assert_eq!(zigzag_decode(1), -1);
    assert_eq!(zigzag_decode(2), 1);
    assert_eq!(zigzag_decode(3), -2);
    assert_eq!(zigzag_decode(4), 2);
}

#[test]
fn int_roundtrip() {
    for val in [0i64, 1, -1, 127, -128, 1000, -1000, i64::MAX, i64::MIN] {
        let buf = encode_value_with_pre(
            |s| preencode_int(s, val),
            |s| encode_int(s, val),
        );
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_int(&mut state).unwrap(), val);
    }
}

#[test]
fn float64_roundtrip() {
    for val in [0.0f64, 1.5, -1.5, std::f64::consts::PI, f64::MAX, f64::MIN] {
        let buf = encode_value_with_pre(
            |s| preencode_float64(s, val),
            |s| encode_float64(s, val),
        );
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_float64(&mut state).unwrap(), val);
    }
}

#[test]
fn bool_roundtrip() {
    for val in [true, false] {
        let buf = encode_value_with_pre(
            |s| preencode_bool(s, val),
            |s| encode_bool(s, val),
        );
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_bool(&mut state).unwrap(), val);
    }
}

#[test]
fn buffer_some() {
    let data = b"hello world";
    let buf = encode_value_with_pre(
        |s| preencode_buffer(s, Some(data.as_slice())),
        |s| encode_buffer(s, Some(data.as_slice())),
    );
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_buffer(&mut state).unwrap(), Some(data.to_vec()));
}

#[test]
fn buffer_none() {
    let buf = encode_value_with_pre(
        |s| preencode_buffer(s, None),
        |s| encode_buffer(s, None),
    );
    assert_eq!(buf, vec![0x00]);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_buffer(&mut state).unwrap(), None);
}

#[test]
fn string_roundtrip() {
    for val in ["", "hello", "hello world 🌍", "a".repeat(1000).as_str()] {
        let buf = encode_value_with_pre(
            |s| preencode_string(s, val),
            |s| encode_string(s, val),
        );
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_string(&mut state).unwrap(), val);
    }
}

#[test]
fn fixed32_roundtrip() {
    let data = [42u8; 32];
    let buf = encode_value_with_pre(
        |s| { preencode_fixed32(s, &data).unwrap(); },
        |s| encode_fixed32(s, &data),
    );
    assert_eq!(buf.len(), 32);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_fixed32(&mut state).unwrap(), data);
}

#[test]
fn fixed64_roundtrip() {
    let data = [99u8; 64];
    let buf = encode_value_with_pre(
        |s| { preencode_fixed64(s, &data).unwrap(); },
        |s| encode_fixed64(s, &data),
    );
    assert_eq!(buf.len(), 64);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_fixed64(&mut state).unwrap(), data);
}

#[test]
fn ipv4_roundtrip() {
    for addr in ["127.0.0.1", "192.168.1.1", "0.0.0.0", "255.255.255.255"] {
        let buf = encode_value_with_pre(
            |s| preencode_ipv4(s, addr),
            |s| encode_ipv4(s, addr).unwrap(),
        );
        assert_eq!(buf.len(), 4);
        let mut state = State::from_buffer(&buf);
        assert_eq!(decode_ipv4(&mut state).unwrap(), addr);
    }
}

#[test]
fn ipv6_roundtrip() {
    let buf = encode_value_with_pre(
        |s| preencode_ipv6(s, "::1"),
        |s| encode_ipv6(s, "::1").unwrap(),
    );
    assert_eq!(buf.len(), 16);
    let mut state = State::from_buffer(&buf);
    let decoded = decode_ipv6(&mut state).unwrap();
    assert_eq!(decoded, "::1");
}

#[test]
fn ip_dual_v4() {
    let addr = "192.168.1.1";
    let buf = encode_value_with_pre(
        |s| preencode_ip(s, addr),
        |s| encode_ip(s, addr).unwrap(),
    );
    assert_eq!(buf[0], 4);
    assert_eq!(buf.len(), 5);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_ip(&mut state).unwrap(), addr);
}

#[test]
fn ip_dual_v6() {
    let addr = "::1";
    let buf = encode_value_with_pre(
        |s| preencode_ip(s, addr),
        |s| encode_ip(s, addr).unwrap(),
    );
    assert_eq!(buf[0], 6);
    assert_eq!(buf.len(), 17);
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_ip(&mut state).unwrap(), "::1");
}

#[test]
fn uint_array_roundtrip() {
    let arr = vec![0u64, 1, 252, 253, 65535, 65536, 0xffffffff, 0x100000000];
    let buf = encode_value_with_pre(
        |s| preencode_uint_array(s, &arr),
        |s| encode_uint_array(s, &arr),
    );
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_uint_array(&mut state).unwrap(), arr);
}

#[test]
fn string_array_roundtrip() {
    let arr = vec!["hello", "world", "", "🌍"];
    let buf = encode_value_with_pre(
        |s| preencode_string_array(s, &arr),
        |s| encode_string_array(s, &arr),
    );
    let mut state = State::from_buffer(&buf);
    assert_eq!(decode_string_array(&mut state).unwrap(), arr.iter().map(|s| s.to_string()).collect::<Vec<_>>());
}

#[test]
fn ipv4_address_roundtrip() {
    let buf = encode_value_with_pre(
        |s| preencode_ipv4_address(s, "10.0.0.1", 8080),
        |s| encode_ipv4_address(s, "10.0.0.1", 8080).unwrap(),
    );
    assert_eq!(buf.len(), 6);
    let mut state = State::from_buffer(&buf);
    let (addr, port) = decode_ipv4_address(&mut state).unwrap();
    assert_eq!(addr, "10.0.0.1");
    assert_eq!(port, 8080);
}

#[test]
fn out_of_bounds_error() {
    let mut state = State::from_buffer(&[]);
    assert!(decode_uint8(&mut state).is_err());

    let mut state = State::from_buffer(&[0x01]);
    assert!(decode_uint16(&mut state).is_err());
}

#[test]
fn multiple_values_sequential() {
    let mut state = State::new();
    preencode_uint(state.borrow_mut(), 42);
    preencode_string(&mut state, "hello");
    preencode_bool(&mut state, true);
    state.alloc();
    encode_uint(&mut state, 42);
    encode_string(&mut state, "hello");
    encode_bool(&mut state, true);

    let mut dec = State::from_buffer(&state.buffer);
    assert_eq!(decode_uint(&mut dec).unwrap(), 42);
    assert_eq!(decode_string(&mut dec).unwrap(), "hello");
    assert!(decode_bool(&mut dec).unwrap());
    assert_eq!(dec.start, dec.end);
}

trait BorrowMut {
    fn borrow_mut(&mut self) -> &mut Self {
        self
    }
}

impl BorrowMut for State {}

mod golden_interop {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct GoldenFile {
        #[allow(dead_code)]
        generated_by: String,
        #[allow(dead_code)]
        version: String,
        fixtures: Vec<Fixture>,
    }

    #[derive(Deserialize)]
    struct Fixture {
        #[serde(rename = "type")]
        typ: String,
        label: String,
        value: serde_json::Value,
        hex: String,
    }

    fn load_fixtures() -> Vec<Fixture> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/interop/golden-fixtures.json"
        );
        let data = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Failed to read golden fixtures at {path}: {e}. Run `node generate-golden.js` in tests/node/ first."));
        let file: GoldenFile = serde_json::from_str(&data)
            .unwrap_or_else(|e| panic!("Failed to parse golden fixtures: {e}"));
        file.fixtures
    }

    fn expected_bytes(hex_str: &str) -> Vec<u8> {
        hex::decode(hex_str).unwrap_or_else(|e| panic!("Invalid hex '{hex_str}': {e}"))
    }

    fn encode_with_pre(pre_fn: impl FnOnce(&mut State), enc_fn: impl FnOnce(&mut State)) -> Vec<u8> {
        let mut state = State::new();
        pre_fn(&mut state);
        state.alloc();
        enc_fn(&mut state);
        state.buffer
    }

    fn val_u64(v: &serde_json::Value) -> u64 {
        match v {
            serde_json::Value::Number(n) => n.as_u64().unwrap(),
            serde_json::Value::String(s) => s.parse::<u64>().unwrap(),
            _ => panic!("Expected number or string for u64, got {v:?}"),
        }
    }

    fn val_i64(v: &serde_json::Value) -> i64 {
        match v {
            serde_json::Value::Number(n) => n.as_i64().unwrap(),
            serde_json::Value::String(s) => s.parse::<i64>().unwrap(),
            _ => panic!("Expected number or string for i64, got {v:?}"),
        }
    }

    fn val_f64(v: &serde_json::Value) -> f64 {
        v.as_f64().unwrap()
    }

    fn val_f32(v: &serde_json::Value) -> f32 {
        v.as_f64().unwrap() as f32
    }

    fn val_str(v: &serde_json::Value) -> &str {
        v.as_str().unwrap()
    }

    fn val_bool(v: &serde_json::Value) -> bool {
        v.as_bool().unwrap()
    }

    #[test]
    fn golden_uint8() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint8") {
            let val = val_u64(&f.value) as u8;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_uint8(s, val), |s| encode_uint8(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint8(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}: expected {val}, got {decoded}", f.label);
        }
    }

    #[test]
    fn golden_uint16() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint16") {
            let val = val_u64(&f.value) as u16;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_uint16(s, val), |s| encode_uint16(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint16(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_uint24() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint24") {
            let val = val_u64(&f.value) as u32;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_uint24(s, val), |s| encode_uint24(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint24(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_uint32() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint32") {
            let val = val_u64(&f.value) as u32;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_uint32(s, val), |s| encode_uint32(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint32(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_uint64() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint64") {
            let val = val_u64(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_uint64(s, val), |s| encode_uint64(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint64(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_uint_varint() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint") {
            let val = val_u64(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_uint(s, val), |s| encode_uint(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_int_zigzag() {
        for f in load_fixtures().iter().filter(|f| f.typ == "int") {
            let val = val_i64(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_int(s, val), |s| encode_int(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_int(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_int8() {
        for f in load_fixtures().iter().filter(|f| f.typ == "int8") {
            let val = val_i64(&f.value) as i8;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_int8(s, val), |s| encode_int8(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_int8(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_int16() {
        for f in load_fixtures().iter().filter(|f| f.typ == "int16") {
            let val = val_i64(&f.value) as i16;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_int16(s, val), |s| encode_int16(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_int16(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_int32() {
        for f in load_fixtures().iter().filter(|f| f.typ == "int32") {
            let val = val_i64(&f.value) as i32;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_int32(s, val), |s| encode_int32(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_int32(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_int64() {
        for f in load_fixtures().iter().filter(|f| f.typ == "int64") {
            let val = val_i64(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_int64(s, val), |s| encode_int64(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_int64(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_float32() {
        for f in load_fixtures().iter().filter(|f| f.typ == "float32") {
            let val = val_f32(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_float32(s, val), |s| encode_float32(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_float32(&mut state).unwrap();
            assert_eq!(decoded.to_bits(), val.to_bits(), "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_float64() {
        for f in load_fixtures().iter().filter(|f| f.typ == "float64") {
            let val = val_f64(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_float64(s, val), |s| encode_float64(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_float64(&mut state).unwrap();
            assert_eq!(decoded.to_bits(), val.to_bits(), "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_bool() {
        for f in load_fixtures().iter().filter(|f| f.typ == "bool") {
            let val = val_bool(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_bool(s, val), |s| encode_bool(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_bool(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_buffer() {
        for f in load_fixtures().iter().filter(|f| f.typ == "buffer") {
            let expected = expected_bytes(&f.hex);

            let val: Option<Vec<u8>> = if f.value.is_null() {
                None
            } else {
                let hex_str = val_str(&f.value);
                if hex_str.is_empty() {
                    None
                } else {
                    Some(hex::decode(hex_str).unwrap())
                }
            };

            let val_slice: Option<&[u8]> = val.as_deref();
            let encoded = encode_with_pre(
                |s| preencode_buffer(s, val_slice),
                |s| encode_buffer(s, val_slice),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_buffer(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_string() {
        for f in load_fixtures().iter().filter(|f| f.typ == "string") {
            let val = val_str(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(|s| preencode_string(s, val), |s| encode_string(s, val));
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_string(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_fixed32() {
        for f in load_fixtures().iter().filter(|f| f.typ == "fixed32") {
            let val_vec = hex::decode(val_str(&f.value)).unwrap();
            let val: [u8; 32] = val_vec.try_into().unwrap();
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| { preencode_fixed32(s, &val).unwrap(); },
                |s| encode_fixed32(s, &val),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_fixed32(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_fixed64() {
        for f in load_fixtures().iter().filter(|f| f.typ == "fixed64") {
            let val_vec = hex::decode(val_str(&f.value)).unwrap();
            let val: [u8; 64] = val_vec.try_into().unwrap();
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| { preencode_fixed64(s, &val).unwrap(); },
                |s| encode_fixed64(s, &val),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_fixed64(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_ipv4() {
        for f in load_fixtures().iter().filter(|f| f.typ == "ipv4") {
            let val = val_str(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| preencode_ipv4(s, val),
                |s| encode_ipv4(s, val).unwrap(),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_ipv4(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_ipv6() {
        for f in load_fixtures().iter().filter(|f| f.typ == "ipv6") {
            let val = val_str(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| preencode_ipv6(s, val),
                |s| encode_ipv6(s, val).unwrap(),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_ipv6(&mut state).unwrap();
            let expected_addr: std::net::Ipv6Addr = val.parse().unwrap();
            let decoded_addr: std::net::Ipv6Addr = decoded.parse().unwrap();
            assert_eq!(decoded_addr, expected_addr, "DECODE {}: expected {expected_addr}, got {decoded_addr}", f.label);
        }
    }

    #[test]
    fn golden_ip_dual() {
        for f in load_fixtures().iter().filter(|f| f.typ == "ip") {
            let val = val_str(&f.value);
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| preencode_ip(s, val),
                |s| encode_ip(s, val).unwrap(),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_ip(&mut state).unwrap();

            if val.contains(':') {
                let expected_addr: std::net::Ipv6Addr = val.parse().unwrap();
                let decoded_addr: std::net::Ipv6Addr = decoded.parse().unwrap();
                assert_eq!(decoded_addr, expected_addr, "DECODE {}", f.label);
            } else {
                assert_eq!(decoded, val, "DECODE {}", f.label);
            }
        }
    }

    #[test]
    fn golden_ipv4_address() {
        for f in load_fixtures().iter().filter(|f| f.typ == "ipv4Address") {
            let obj = f.value.as_object().unwrap();
            let host = obj["host"].as_str().unwrap();
            let port = obj["port"].as_u64().unwrap() as u16;
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| preencode_ipv4_address(s, host, port),
                |s| encode_ipv4_address(s, host, port).unwrap(),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let (decoded_host, decoded_port) = decode_ipv4_address(&mut state).unwrap();
            assert_eq!(decoded_host, host, "DECODE host {}", f.label);
            assert_eq!(decoded_port, port, "DECODE port {}", f.label);
        }
    }

    #[test]
    fn golden_uint_array() {
        for f in load_fixtures().iter().filter(|f| f.typ == "uint_array") {
            let val: Vec<u64> = f.value.as_array().unwrap().iter().map(val_u64).collect();
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| preencode_uint_array(s, &val),
                |s| encode_uint_array(s, &val),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_uint_array(&mut state).unwrap();
            assert_eq!(decoded, val, "DECODE {}", f.label);
        }
    }

    #[test]
    fn golden_string_array() {
        for f in load_fixtures().iter().filter(|f| f.typ == "string_array") {
            let val: Vec<&str> = f.value.as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
            let expected = expected_bytes(&f.hex);
            let encoded = encode_with_pre(
                |s| preencode_string_array(s, &val),
                |s| encode_string_array(s, &val),
            );
            assert_eq!(encoded, expected, "ENCODE {}: expected {}, got {}", f.label, f.hex, hex::encode(&encoded));

            let mut state = State::from_buffer(&expected);
            let decoded = decode_string_array(&mut state).unwrap();
            let val_owned: Vec<String> = val.iter().map(|s| s.to_string()).collect();
            assert_eq!(decoded, val_owned, "DECODE {}", f.label);
        }
    }
}
