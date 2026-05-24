#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kaya_lsm::fuzz_decode_data_block(data);
    kaya_lsm::fuzz_decode_index_block(data);
});
