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

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;

pub mod nydus;
pub mod raw;

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug)]
pub(crate) struct FsReadMetrics {
    image_type: &'static str,
    read_total: Counter<u64>,
    read_duration: Histogram<f64>,
    read_bytes: Counter<u64>,
}

impl FsReadMetrics {
    pub(crate) fn new(image_type: &'static str) -> Self {
        let meter = global::meter("distill_fs.fs");
        Self::with_meter(&meter, image_type)
    }

    pub(crate) fn with_meter(
        meter: &opentelemetry::metrics::Meter,
        image_type: &'static str,
    ) -> Self {
        Self {
            image_type,
            read_total: meter
                .u64_counter("distill_fs.fs.read_total")
                .with_description("Total filesystem read attempts")
                .init(),
            read_duration: meter
                .f64_histogram("distill_fs.fs.read_duration_ms")
                .with_description("Filesystem read duration")
                .with_unit("ms")
                .init(),
            read_bytes: meter
                .u64_counter("distill_fs.fs.read_bytes")
                .with_description("Bytes returned by filesystem reads")
                .with_unit("By")
                .init(),
        }
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn record_read(&self, result: &'static str, elapsed_ms: f64, bytes: usize) {
        let attrs = [
            KeyValue::new("image_type", self.image_type),
            KeyValue::new("result", result),
        ];
        self.read_total.add(1, &attrs);
        self.read_duration.record(elapsed_ms, &attrs);
        if bytes > 0 {
            self.read_bytes.add(bytes as u64, &attrs);
        }
    }
}

#[cfg(test)]
mod tests;
