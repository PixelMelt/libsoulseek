use std::net::SocketAddr;

pub const DEFAULT_SERVER: &str = "server.slsknet.org:2242";
pub const DEFAULT_UPLOAD_SLOTS: usize = 2;
pub const DEFAULT_QUEUE_FILE_LIMIT: usize = 500;
pub const DEFAULT_MAX_SEARCH_RESULTS: usize = 300;
pub const DEFAULT_MIN_SEARCH_CHARS: usize = 3;
pub const DEFAULT_BANNED_MESSAGE: &str = "Banned";

const MAX_UPLOAD_SLOTS: usize = 1_000_000;
const MAX_QUEUE_SIZE_LIMIT_MB: u64 = 1 << 30;

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    pub login: LoginConfig,
    pub auto_reconnect: bool,
    pub transfers: TransferConfig,
    pub search: SearchConfig,
}

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), String> {
        let transfers = &self.transfers;
        if transfers.upload_slots == 0 || transfers.upload_slots > MAX_UPLOAD_SLOTS {
            return Err(format!("upload_slots must be 1-{MAX_UPLOAD_SLOTS}"));
        }
        if transfers.queue_file_limit == 0 {
            return Err("queue_file_limit must be at least 1".into());
        }
        if transfers.queue_size_limit_mb > MAX_QUEUE_SIZE_LIMIT_MB {
            return Err(format!(
                "queue_size_limit_mb {} is invalid: the limit is bounded to \
                 0-{MAX_QUEUE_SIZE_LIMIT_MB}",
                transfers.queue_size_limit_mb
            ));
        }
        if transfers.banned_message.is_empty() {
            return Err("banned_message must not be empty".into());
        }
        let max_search_results = self.search.max_search_results;
        if max_search_results == 0 || max_search_results > DEFAULT_MAX_SEARCH_RESULTS {
            return Err(format!(
                "max_search_results {max_search_results} is invalid: the protocol answer cap \
                 is 1-{DEFAULT_MAX_SEARCH_RESULTS}"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoginConfig {
    pub server: SocketAddr,
    pub username: String,
    pub password: String,
    pub listen_port: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferConfig {
    pub upload_slots: usize,
    pub queue_file_limit: usize,
    pub upload_limit_kbps: u32,
    pub download_limit_kbps: u32,
    pub queue_size_limit_mb: u64,
    pub banned_message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchConfig {
    pub max_search_results: usize,
    pub min_search_chars: usize,
    pub respond_to_searches: bool,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            max_search_results: DEFAULT_MAX_SEARCH_RESULTS,
            min_search_chars: DEFAULT_MIN_SEARCH_CHARS,
            respond_to_searches: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RuntimeConfig {
        RuntimeConfig {
            login: LoginConfig {
                server: "127.0.0.1:2242".parse().unwrap(),
                username: "someone".into(),
                password: "secret".into(),
                listen_port: 2234,
            },
            auto_reconnect: true,
            transfers: TransferConfig {
                upload_slots: DEFAULT_UPLOAD_SLOTS,
                queue_file_limit: DEFAULT_QUEUE_FILE_LIMIT,
                upload_limit_kbps: 0,
                download_limit_kbps: 0,
                queue_size_limit_mb: 0,
                banned_message: DEFAULT_BANNED_MESSAGE.into(),
            },
            search: SearchConfig::default(),
        }
    }

    #[test]
    fn transfer_limits_stay_in_range() {
        let with_transfers = |edit: fn(&mut TransferConfig)| {
            let mut config = config();
            edit(&mut config.transfers);
            config
        };
        let invalid: [fn(&mut TransferConfig); 5] = [
            |transfers| transfers.upload_slots = 0,
            |transfers| transfers.upload_slots = MAX_UPLOAD_SLOTS + 1,
            |transfers| transfers.queue_file_limit = 0,
            |transfers| transfers.queue_size_limit_mb = MAX_QUEUE_SIZE_LIMIT_MB + 1,
            |transfers| transfers.banned_message.clear(),
        ];
        for edit in invalid {
            assert!(with_transfers(edit).validate().is_err());
        }
        let valid: [fn(&mut TransferConfig); 3] = [
            |transfers| transfers.upload_slots = MAX_UPLOAD_SLOTS,
            |transfers| transfers.queue_file_limit = 1,
            |transfers| transfers.queue_size_limit_mb = MAX_QUEUE_SIZE_LIMIT_MB,
        ];
        for edit in valid {
            assert!(with_transfers(edit).validate().is_ok());
        }
    }

    #[test]
    fn search_answers_stay_within_the_protocol_cap() {
        let with_max = |max_search_results| RuntimeConfig {
            search: SearchConfig {
                max_search_results,
                ..SearchConfig::default()
            },
            ..config()
        };
        assert!(with_max(0).validate().is_err());
        assert!(with_max(DEFAULT_MAX_SEARCH_RESULTS + 1).validate().is_err());
        assert!(with_max(1).validate().is_ok());
        assert!(with_max(DEFAULT_MAX_SEARCH_RESULTS).validate().is_ok());
    }
}
