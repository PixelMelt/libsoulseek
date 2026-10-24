use super::ClientActor;
use crate::client::{ClientEvent, Observation, ProfileRequest};
use crate::protocol::PeerMessage;
use crate::types::Restriction;

impl ClientActor {
    pub(super) fn respond_to_user_info(&mut self, username: String) {
        self.emit(ClientEvent::Observed(Observation::UserInfoRequest {
            username: username.clone(),
        }));
        let banned = self.users.is_banned(&username);
        let (folders, files) = self.sharing.counts();
        let description = self.profile.description(&ProfileRequest {
            user_name: &username,
            user_restriction: self
                .users
                .restriction(&username)
                .unwrap_or(&Restriction::None)
                .as_str(),
            user_queued_files: self.uploads.queued_for(&username),
            user_active_uploads: self.uploads.active_uploads(&username),
            user_is_buddy: self.users.is_buddy(&username),
            user_is_ignored: self.users.is_ignored(&username),
            user_ban_reason: banned.then_some(self.config.transfers.banned_message.as_str()),
            user_is_privileged: self.users.is_privileged(&username),
            my_name: &self.config.login.username,
            my_shared_files: files,
            my_shared_folders: folders,
            my_queue_size: self.uploads.queue_size(),
            my_slots: self.uploads.total_slots(),
            my_free_slots: self.uploads.free_slots(),
            my_upload_speed: self.uploads.upload_speed,
        });
        let response = if banned {
            PeerMessage::UserInfoResponse {
                description,
                picture: None,
                total_uploads: 0,
                queue_size: 0,
                slots_available: false,
                upload_allowed: Some(0),
            }
        } else {
            PeerMessage::UserInfoResponse {
                description,
                picture: None,
                total_uploads: self.uploads.total_slots(),
                queue_size: self.uploads.queue_size(),
                slots_available: self.uploads.is_new_upload_accepted(),
                upload_allowed: Some(0),
            }
        };
        self.net.peer(username, response);
    }
}
