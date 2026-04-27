use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

// TODO: random magic suffix changes at random times, sometimes a week, sometimes more.
// Soooo we need to implement some kind of "fallback" mechanism and sup port extra fetchers.
// 1. Add CLI arg for magic suffix
// 2. Implement simple service that will yield valid magic suffixes
// 3. Implement module with some JS runtime (mb rquickjs) and evaluate magic in there, but wt.obf.js has a decent anti-tampering mechanism
const MAGIC_SUFFIX: &str = "5d4f7g8sd45fsd";
#[derive(Clone)]
pub struct WtGenerator {
    base_hasher: Sha256,
}

/// Implementation of https://gofile.io/dist/js/wt.obf.js
impl WtGenerator {
    /// Creates a new generator with token, user-agent, and language as the fixed prefix
    pub fn new(token: &str, user_agent: &str, language: &str) -> Self {
        let prefix = format!("{user_agent}::{language}::{token}::");
        let mut hasher = Sha256::new();
        hasher.update(prefix.as_bytes());

        Self {
            base_hasher: hasher,
        }
    }

    /// Generates the hash for the current time
    pub fn generate_current(&self) -> String {
        self.generate_for_time(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
    }

    /// Generates the hash for a specific timestamp
    #[inline(always)]
    fn generate_for_time(&self, timestamp_sec: u64) -> String {
        let mut hasher = self.base_hasher.clone();

        // Use a 4-hour time window
        let time_window = timestamp_sec / 14_400;
        hasher.update(time_window.to_string().as_bytes());
        hasher.update(format!("::{MAGIC_SUFFIX}").as_bytes());

        format!("{:x}", hasher.finalize())
    }

    /// Allows injecting a fixed time for deterministic testing
    #[cfg(test)]
    pub fn generate_with_mocked_time<F>(&self, time_fn: F) -> String
    where
        F: Fn() -> u64,
    {
        self.generate_for_time(time_fn())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wt_generator_with_fixed_time() {
        let token = "dMYWD3s9oHw1fUTbVEYgVX0o2eTy2zmd";
        let user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)";
        let language = "en-US";

        let generator = WtGenerator::new(token, user_agent, language);

        let fixed_timestamp = 1_772_811_652u64;
        let expected_hash = "cf132b6c2ab66415d2aaabe7008eefd085c942f276d7f5670dfc010e13d6ed40";

        assert_eq!(
            generator.generate_with_mocked_time(|| fixed_timestamp),
            expected_hash
        );
    }
}
