#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = kaya_net::decode_envelope(data);
    let _ = kaya_net::decode_put_payload(data);
    let _ = kaya_net::decode_key_payload(data);
    let _ = kaya_net::decode_scan_payload(data);
    let _ = kaya_net::decode_scan_response(data);
    let _ = kaya_net::decode_value_payload(data);
    let _ = kaya_net::decode_error_payload(data);
});
