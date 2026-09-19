// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

/// Parse whole bytes or an integer followed by B, KiB, MiB, GiB, or TiB.
pub fn parse_chunk_db_size(value: &str) -> Result<usize, String> {
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let multiplier = match unit {
        "" | "B" => 1_u64,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return Err("use whole bytes or an integer with B, KiB, MiB, GiB, TiB".into()),
    };
    let size = number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| "invalid or overflowing ChunkDB size".to_string())?;
    validate_size(size).map_err(|err| err.to_string())?;
    Ok(size)
}

pub(super) fn validate_size(size: usize) -> anyhow::Result<()> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    anyhow::ensure!(page_size > 0, "cannot determine system page size");
    anyhow::ensure!(size >= 1024 * 1024, "ChunkDB size must be at least 1MiB");
    anyhow::ensure!(
        size <= isize::MAX as usize,
        "ChunkDB size exceeds addressable range"
    );
    anyhow::ensure!(
        size % page_size as usize == 0,
        "ChunkDB size must be a multiple of the system page size ({page_size} bytes)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sizes_are_checked_without_rounding() {
        assert_eq!(parse_chunk_db_size("64MiB").unwrap(), 64 << 20);
        assert_eq!(parse_chunk_db_size("1048576").unwrap(), 1 << 20);
        for value in [
            "0",
            "-1",
            "1Mi",
            "1.5GiB",
            "1MB",
            "1048577",
            "1KiB",
            "18446744073709551615TiB",
            "",
        ] {
            assert!(parse_chunk_db_size(value).is_err(), "{value}");
        }
    }
}
