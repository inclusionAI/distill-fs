// Copyright (c) 2026 Ant Group Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::test_metrics::{histogram_points_f64, sum_points_u64, MetricsHarness};
use std::collections::BTreeMap;

fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[test]
fn fs_read_metrics_emit_raw_labels() {
    let harness = MetricsHarness::new();
    let metrics = FsReadMetrics::with_meter(&harness.meter("distill_fs.fs.test"), "raw");

    metrics.record_read("ok", 12.5, 4096);

    let collected = harness.collect();
    assert!(sum_points_u64(&collected, "distill_fs.fs.read_total")
        .contains(&(attrs(&[("image_type", "raw"), ("result", "ok")]), 1)));
    assert!(sum_points_u64(&collected, "distill_fs.fs.read_bytes")
        .contains(&(attrs(&[("image_type", "raw"), ("result", "ok")]), 4096)));
    assert!(
        histogram_points_f64(&collected, "distill_fs.fs.read_duration_ms").contains(&(
            attrs(&[("image_type", "raw"), ("result", "ok")]),
            1,
            12.5
        ))
    );
}

#[test]
fn fs_read_metrics_emit_nydus_labels() {
    let harness = MetricsHarness::new();
    let metrics = FsReadMetrics::with_meter(&harness.meter("distill_fs.fs.test"), "nydus");

    metrics.record_read("error", 7.0, 0);

    let collected = harness.collect();
    assert!(sum_points_u64(&collected, "distill_fs.fs.read_total")
        .contains(&(attrs(&[("image_type", "nydus"), ("result", "error")]), 1)));
    assert!(
        histogram_points_f64(&collected, "distill_fs.fs.read_duration_ms").contains(&(
            attrs(&[("image_type", "nydus"), ("result", "error")]),
            1,
            7.0
        ))
    );
}
