use std::collections::HashSet;

const BACKSLASH_SENTINEL: &str = "@@BACKSLASH@@";

#[derive(Debug, Clone)]
pub struct ShareFilters {
    names: HashSet<String>,
}

impl ShareFilters {
    pub fn new(filters: Vec<String>) -> Result<Self, String> {
        for filter in &filters {
            if filter.is_empty() || filter.contains(['/', '\\']) {
                return Err(format!(
                    "share filter {filter:?} is invalid: filters match exact file or \
                     folder names and cannot be empty or contain path separators"
                ));
            }
        }
        Ok(Self {
            names: filters.into_iter().collect(),
        })
    }

    pub fn skips(&self, name: &str) -> bool {
        name.starts_with('.') || self.names.contains(name)
    }
}

pub fn escape_backslashes(name: &str) -> String {
    name.replace('\\', BACKSLASH_SENTINEL)
}

pub fn check_share_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains(['/', '\\']) {
        return Err(format!(
            "share name {name:?} is invalid: remote clients rewrite slashes in \
             virtual paths and their download requests would no longer match"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backslashes_in_real_names_become_the_sentinel() {
        assert_eq!(
            escape_backslashes("a\\b\\c"),
            "a@@BACKSLASH@@b@@BACKSLASH@@c"
        );
        assert_eq!(escape_backslashes("plain"), "plain");
    }

    #[test]
    fn share_names_and_filters_follow_the_protocol_rules() {
        for name in ["/", "a/b", "a\\b", ""] {
            assert!(check_share_name(name).is_err(), "{name:?} must be rejected");
        }
        for filter in ["", "a/b", "a\\b"] {
            assert!(
                ShareFilters::new(vec![filter.into()]).is_err(),
                "{filter:?} must be rejected"
            );
        }
        assert!(check_share_name("Music").is_ok());
        assert!(ShareFilters::new(vec!["desktop.ini".into()]).is_ok());
    }

    #[test]
    fn filters_skip_hidden_and_exactly_matching_names() {
        let filters = ShareFilters::new(vec!["Thumbs.db".into()]).unwrap();
        assert!(filters.skips(".hidden"));
        assert!(filters.skips("Thumbs.db"));
        assert!(!filters.skips("thumbs.db"));
        assert!(!filters.skips("song.mp3"));
    }
}
