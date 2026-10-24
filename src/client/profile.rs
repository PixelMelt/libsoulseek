pub trait ProfileSource: Send + Sync {
    fn description(&self, request: &ProfileRequest<'_>) -> String;
}

pub struct ProfileRequest<'a> {
    pub user_name: &'a str,
    pub user_restriction: &'static str,
    pub user_queued_files: u32,
    pub user_active_uploads: u32,
    pub user_is_buddy: bool,
    pub user_is_ignored: bool,
    pub user_ban_reason: Option<&'a str>,
    pub user_is_privileged: bool,
    pub my_name: &'a str,
    pub my_shared_files: u32,
    pub my_shared_folders: u32,
    pub my_queue_size: u32,
    pub my_slots: u32,
    pub my_free_slots: u32,
    pub my_upload_speed: u32,
}
