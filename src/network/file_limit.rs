const MAX_FILE_LIMIT: libc::rlim_t = 10240;
const MAX_SOCKETS: libc::rlim_t = 2048;

pub(super) fn raise_file_limit() -> usize {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let read = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    assert_eq!(
        read,
        0,
        "read the open file limit: {}",
        std::io::Error::last_os_error()
    );
    let wanted = limit.rlim_max.min(MAX_FILE_LIMIT);
    if limit.rlim_cur < wanted {
        limit.rlim_cur = wanted;
        let raised = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
        assert_eq!(
            raised,
            0,
            "raise the open file limit to {wanted}: {}",
            std::io::Error::last_os_error()
        );
    }
    socket_budget(limit.rlim_cur)
}

fn socket_budget(file_limit: libc::rlim_t) -> usize {
    (file_limit.saturating_mul(2) / 3).min(MAX_SOCKETS) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sockets_get_two_thirds_of_the_file_limit_up_to_2048() {
        assert_eq!(socket_budget(1024), 682);
        assert_eq!(socket_budget(3072), 2048);
        assert_eq!(socket_budget(10240), 2048);
        assert_eq!(socket_budget(libc::RLIM_INFINITY), 2048);
    }

    #[test]
    fn raising_leaves_the_soft_limit_at_least_the_capped_hard_limit() {
        let budget = raise_file_limit();
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
            0
        );
        assert!(limit.rlim_cur >= limit.rlim_max.min(MAX_FILE_LIMIT));
        assert_eq!(budget, socket_budget(limit.rlim_cur));
    }
}
