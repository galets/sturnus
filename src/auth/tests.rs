use super::*;
use hyper::header::HeaderValue;

fn headers_with(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    // Values here are ASCII, so building the header can't fail.
    headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
    headers
}

fn keys(const_keys: &[&str]) -> ApiKeys {
    let configured: Vec<String> = const_keys.iter().map(|key| (*key).to_string()).collect();
    ApiKeys::from_config(&configured).unwrap()
}

#[test]
fn no_configured_keys_means_no_gate() {
    assert!(ApiKeys::from_config(&[]).is_none());
}

#[test]
fn the_configured_key_is_accepted_in_any_scheme_case() {
    // RFC 7235 defines the auth scheme as case-insensitive; SDKs send both.
    let api_keys = keys(&["secret-key"]);
    for value in [
        "Bearer secret-key",
        "bearer secret-key",
        "BEARER secret-key",
    ] {
        assert!(api_keys.is_authorized(&headers_with(value)), "{value}");
    }
}

#[test]
fn any_configured_key_authorizes() {
    let api_keys = keys(&["first-key", "second-key", "third-key"]);
    for key in ["first-key", "second-key", "third-key"] {
        assert!(
            api_keys.is_authorized(&headers_with(&format!("Bearer {key}"))),
            "{key} is configured, so it must authorize"
        );
    }
}

#[test]
fn anything_but_an_exact_configured_key_is_refused() {
    let api_keys = keys(&["secret-key"]);
    for value in [
        "secret-key",             // no scheme
        "Basic c2VjcmV0LWtleQ==", // a scheme we do not speak
        "Bearer secret-ke",       // prefix of the key
        "Bearer secret-keys",     // key with a tail
        "Bearer other-key",
        "Bearer  secret-key", // the token is what it is
        "Bearer",
        "Bearer ",
        "",
    ] {
        assert!(
            !api_keys.is_authorized(&headers_with(value)),
            "{value:?} must not authorize"
        );
    }
}

#[test]
fn a_key_never_authorizes_another_instance() {
    // Same bytes, different deployment: the gate is only what it was configured
    // with, never a default or an empty comparison.
    let api_keys = keys(&["sk-live-9f2c"]);
    assert!(!api_keys.is_authorized(&headers_with("Bearer sk-live-9f2d")));
    assert!(api_keys.is_authorized(&headers_with("Bearer sk-live-9f2c")));
}

#[test]
fn a_non_ascii_header_is_refused_rather_than_panicking() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_bytes(b"Bearer s\xc3\xa9cret").unwrap(),
    );
    assert!(!keys(&["secret"]).is_authorized(&headers));
}
