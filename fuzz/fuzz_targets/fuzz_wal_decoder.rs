#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = kaya_wal::decode_record(data, 0, u32::MAX);
});
