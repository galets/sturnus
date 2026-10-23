//! Caller-facing API key authentication: the gate in front of the proxy.
//!
//! Keys come from the top-level `api_keys` config list. Nothing else reads
//! them, so no log, metric, or proxied response can echo one back.

use hyper::header::{HeaderMap, AUTHORIZATION};

/// The keys a caller may present in `Authorization: Bearer <key>`.
pub struct ApiKeys {
    keys: Vec<String>,
}

impl ApiKeys {
    /// `None` when nothing is configured, keeping "this instance does not
    /// authenticate callers" a type-level fact rather than an empty set that
    /// happens to reject everything.
    pub fn from_config(keys: &[String]) -> Option<Self> {
        (!keys.is_empty()).then(|| Self {
            keys: keys.to_vec(),
        })
    }

    /// Whether a request carries one of the configured keys.
    pub fn is_authorized(&self, headers: &HeaderMap) -> bool {
        let Some(presented) = bearer_token(headers) else {
            return false;
        };
        // Every key is compared, so neither a match nor its position in the
        // list shows up in how long the check takes.
        self.keys.iter().fold(false, |matched, key| {
            matched | secret_eq(presented.as_bytes(), key.as_bytes())
        })
    }
}

/// The bearer token of an `Authorization` header, `None` for anything else.
/// The scheme is matched case-insensitively, as RFC 7235 defines it.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    // A non-UTF-8 header is simply not a key we were offered.
    let (scheme, token) = headers.get(AUTHORIZATION)?.to_str().ok()?.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    (!token.is_empty()).then_some(token)
}

/// Compare a presented secret with the expected one, reading both all the way
/// through so a caller cannot bisect a key by timing. A length mismatch is not
/// secret: it is visible to any caller as the length at which a key is found.
fn secret_eq(presented: &[u8], expected: &[u8]) -> bool {
    if presented.len() != expected.len() {
        return false;
    }
    presented
        .iter()
        .zip(expected)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests;
