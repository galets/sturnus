// Metric setup builds static descriptors; an expect failure here is a build-time bug.
#![allow(clippy::expect_used)]

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use crate::router::Placement;

pub struct Metrics {
    registry: Registry,
    pub requests_total: IntCounterVec,
    /// Streaming time-to-first-chunk.
    pub ttfc_seconds: HistogramVec,
    /// Non-streaming full response time.
    pub latency_seconds: HistogramVec,
    pub errors_total: IntCounterVec,
    /// Requests shed with 429 because the aggregate buffer budget was full.
    pub buffer_rejections_total: IntCounter,
    /// Callers that hung up before their request completed. Benign: logged at
    /// debug and counted here so the volume stays observable.
    pub client_disconnects_total: IntCounter,
    /// Requests currently outstanding against each candidate — the concurrency
    /// a caller is queuing behind. Written from the tracker's counts at scrape
    /// time (see [`Metrics::set_in_flight`]), never incremented per request.
    pub in_flight_requests: IntGaugeVec,
    /// Sessions pinned to each candidate in this instance's session table.
    /// Written from the live table at scrape time (see
    /// [`Metrics::set_sessions_pinned`]), like the in-flight gauge is written
    /// from the tracker, so the exported number is the one the request path
    /// consulted and cannot stick after a pin is dropped.
    pub sessions_pinned: IntGaugeVec,
    /// Turns placed, labelled by the reason the placement path reported — one
    /// of the five in [`Placement::ALL`]. Counted at the placement seam, so
    /// "where did this alias's traffic go, and why?" is a query rather than a
    /// log grep, and a pin is visible as a pin rather than blending into the
    /// weighted picks.
    pub placements_total: IntCounterVec,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            Opts::new("sturnus_requests_total", "Total upstream requests"),
            &["alias", "provider", "model", "status_code"],
        )
        .expect("valid metric descriptor");

        // Distinct metrics, not one with a mode label: they measure different things.
        let latency_buckets = vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

        let ttfc_seconds = HistogramVec::new(
            HistogramOpts::new(
                "sturnus_ttfc_seconds",
                "Streaming time-to-first-chunk from upstream",
            )
            .buckets(latency_buckets.clone()),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "sturnus_latency_seconds",
                "Non-streaming full response time from upstream",
            )
            .buckets(latency_buckets),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let errors_total = IntCounterVec::new(
            Opts::new(
                "sturnus_errors_total",
                "Connection failures before reaching upstream",
            ),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let buffer_rejections_total = IntCounter::new(
            "sturnus_buffer_rejections_total",
            "Requests rejected because the aggregate buffer budget was full",
        )
        .expect("valid metric descriptor");

        let client_disconnects_total = IntCounter::new(
            "sturnus_client_disconnects_total",
            "Callers that hung up before their request completed",
        )
        .expect("valid metric descriptor");

        let in_flight_requests = IntGaugeVec::new(
            Opts::new(
                "sturnus_in_flight_requests",
                "Requests currently outstanding against each candidate",
            ),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let sessions_pinned = IntGaugeVec::new(
            Opts::new(
                "sturnus_sessions_pinned",
                "Sessions this instance has pinned to each candidate",
            ),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let placements_total = IntCounterVec::new(
            Opts::new(
                "sturnus_placements_total",
                "Turns placed, by the reason the placement path reported",
            ),
            &["alias", "provider", "model", "reason"],
        )
        .expect("valid metric descriptor");

        registry
            .register(Box::new(requests_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(ttfc_seconds.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(latency_seconds.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(errors_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(buffer_rejections_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(client_disconnects_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(in_flight_requests.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(sessions_pinned.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(placements_total.clone()))
            .expect("unique metric name");

        Self {
            registry,
            requests_total,
            ttfc_seconds,
            latency_seconds,
            errors_total,
            buffer_rejections_total,
            client_disconnects_total,
            in_flight_requests,
            sessions_pinned,
            placements_total,
        }
    }

    /// Zero-init error counters, the two gauges and every placement reason so
    /// a missing series isn't mistaken for "no errors", for "nothing queued",
    /// for "no session is pinned here" or for "this reason never happened".
    pub fn init_zero(&self, aliases: &[(&str, &str, &str)]) {
        for &(alias, provider, model) in aliases {
            self.errors_total
                .with_label_values(&[alias, provider, model]);
            self.in_flight_requests
                .with_label_values(&[alias, provider, model]);
            self.sessions_pinned
                .with_label_values(&[alias, provider, model]);
            for reason in Placement::ALL {
                self.placements_total
                    .with_label_values(&[alias, provider, model, reason.as_str()]);
            }
        }
    }

    /// Refresh the in-flight gauge from the tracker's live counts.
    ///
    /// `set`, not `inc`/`dec`: the count lives in the tracker (it is what
    /// routing reads and what `/status` shows), so writing the exported series
    /// from it at scrape time keeps the two from ever disagreeing, and keeps a
    /// series from sticking at a stale value once its requests are done.
    pub fn set_in_flight(&self, samples: &[(&str, &str, &str, usize)]) {
        for &(alias, provider, model, count) in samples {
            // A gauge takes i64; a count this large would take more than the
            // whole address space to hold in flight.
            let count = i64::try_from(count).unwrap_or(i64::MAX);
            self.in_flight_requests
                .with_label_values(&[alias, provider, model])
                .set(count);
        }
    }

    /// Refresh the sessions-pinned gauge from the live session table.
    ///
    /// `set`, not `inc`/`dec`, for the same reason as [`Metrics::set_in_flight`]:
    /// the count lives in the table (it is what the request path consults and
    /// what `/status` shows), so writing the exported series from it at scrape
    /// time keeps the two from ever disagreeing, and keeps a series from
    /// sticking above zero after a session's pin expires or is evicted.
    pub fn set_sessions_pinned(&self, samples: &[(&str, &str, &str, usize)]) {
        for &(alias, provider, model, count) in samples {
            // A gauge takes i64; the table's cap is far below `i64::MAX`.
            let count = i64::try_from(count).unwrap_or(i64::MAX);
            self.sessions_pinned
                .with_label_values(&[alias, provider, model])
                .set(count);
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_returns_valid_prometheus_text() {
        let m = Metrics::new();
        m.requests_total
            .with_label_values(&["fast", "openai", "gpt-4o-mini", "200"])
            .inc();
        m.ttfc_seconds
            .with_label_values(&["fast", "openai", "gpt-4o-mini"])
            .observe(0.42);
        m.latency_seconds
            .with_label_values(&["fast", "openai", "gpt-4o-mini"])
            .observe(1.5);
        m.errors_total
            .with_label_values(&["fast", "groq", "llama-3"])
            .inc();
        m.client_disconnects_total.inc();
        m.set_in_flight(&[("fast", "openai", "gpt-4o-mini", 2)]);

        let output = String::from_utf8(m.encode().unwrap()).unwrap();
        assert!(output.contains("sturnus_requests_total"));
        assert!(output.contains("sturnus_ttfc_seconds"));
        assert!(output.contains("sturnus_latency_seconds"));
        assert!(output.contains("sturnus_errors_total"));
        assert!(output.contains("sturnus_client_disconnects_total"));
        assert!(output.contains("sturnus_in_flight_requests"));
        assert!(output.contains("alias=\"fast\""));
        assert!(output.contains("provider=\"openai\""));
        assert!(output.contains("status_code=\"200\""));
    }

    /// The gauge is a mirror of the tracker's counts, written at scrape time:
    /// it goes up with them, back down with them, and the series is there at
    /// `0` from startup rather than missing. (The exporter sorts labels
    /// alphabetically, hence the order of the series below.)
    #[test]
    fn in_flight_gauge_mirrors_the_counts_it_is_given() {
        let m = Metrics::new();
        m.init_zero(&[("fast", "openai", "gpt-4o-mini")]);

        let encoded = || String::from_utf8(m.encode().unwrap()).unwrap();
        let series =
            r#"sturnus_in_flight_requests{alias="fast",model="gpt-4o-mini",provider="openai"}"#;

        assert!(
            encoded().contains(&format!("{series} 0")),
            "a zero-initialised series should be exported:\n{}",
            encoded()
        );

        m.set_in_flight(&[("fast", "openai", "gpt-4o-mini", 3)]);
        assert!(
            encoded().contains(&format!("{series} 3")),
            "the count in flight should be exported:\n{}",
            encoded()
        );

        // The count went back to zero, so the gauge follows: no stale value.
        m.set_in_flight(&[("fast", "openai", "gpt-4o-mini", 0)]);
        assert!(
            encoded().contains(&format!("{series} 0")),
            "a finished request must not leave the gauge set:\n{}",
            encoded()
        );
    }

    /// Every placement reason is its own series at `0` from startup, so a
    /// reason with no traffic reads as "this did not happen" rather than as
    /// "no data", and the reason the placement path named is the label.
    #[test]
    fn every_placement_reason_is_zero_initialised() {
        let m = Metrics::new();
        m.init_zero(&[("fast", "openai", "gpt-4o-mini")]);

        let encoded = || String::from_utf8(m.encode().unwrap()).unwrap();
        for reason in Placement::ALL {
            let series = format!(
                r#"sturnus_placements_total{{alias="fast",model="gpt-4o-mini",provider="openai",reason="{}"}}"#,
                reason.as_str()
            );
            assert!(
                encoded().contains(&format!("{series} 0")),
                "{} should be exported at 0 before any traffic:\n{}",
                reason.as_str(),
                encoded()
            );
        }

        m.placements_total
            .with_label_values(&["fast", "openai", "gpt-4o-mini", "pinned"])
            .inc();
        let series = r#"sturnus_placements_total{alias="fast",model="gpt-4o-mini",provider="openai",reason="pinned"}"#;
        assert!(
            encoded().contains(&format!("{series} 1")),
            "the reason a turn rode should be counted:\n{}",
            encoded()
        );
    }

    /// The sessions-pinned gauge is a mirror of the table, written at scrape
    /// time: a pin that expires leaves `0` behind, not a stale count.
    #[test]
    fn sessions_pinned_gauge_mirrors_the_table_it_is_given() {
        let m = Metrics::new();
        m.init_zero(&[("fast", "openai", "gpt-4o-mini")]);

        let encoded = || String::from_utf8(m.encode().unwrap()).unwrap();
        let series =
            r#"sturnus_sessions_pinned{alias="fast",model="gpt-4o-mini",provider="openai"}"#;

        assert!(
            encoded().contains(&format!("{series} 0")),
            "an idle candidate should export a zero pin count:\n{}",
            encoded()
        );

        m.set_sessions_pinned(&[("fast", "openai", "gpt-4o-mini", 4)]);
        assert!(
            encoded().contains(&format!("{series} 4")),
            "the live pin count should be exported:\n{}",
            encoded()
        );

        m.set_sessions_pinned(&[("fast", "openai", "gpt-4o-mini", 0)]);
        assert!(
            encoded().contains(&format!("{series} 0")),
            "a dropped pin must not leave the gauge set:\n{}",
            encoded()
        );
    }
}
