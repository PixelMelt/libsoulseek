use std::collections::VecDeque;

use tokio::sync::mpsc::{self, error::TrySendError};

pub(crate) struct Outbox<T> {
    sender: mpsc::Sender<T>,
    queued: VecDeque<T>,
}

impl<T> Outbox<T> {
    pub(crate) fn new(sender: mpsc::Sender<T>) -> Self {
        Self {
            sender,
            queued: VecDeque::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    pub(crate) fn send(&mut self, item: T) {
        if !self.queued.is_empty() {
            self.queued.push_back(item);
            return;
        }
        if let Err(TrySendError::Full(item)) = self.sender.try_send(item) {
            self.queued.push_back(item);
        }
    }

    pub(crate) async fn ready(&self) {
        let _ = self.sender.reserve().await;
    }

    pub(crate) fn flush(&mut self) {
        while let Some(item) = self.queued.pop_front() {
            match self.sender.try_send(item) {
                Ok(()) => {}
                Err(TrySendError::Full(item)) => {
                    self.queued.push_front(item);
                    return;
                }
                Err(TrySendError::Closed(_)) => {
                    self.queued.clear();
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(rx: &mut mpsc::Receiver<u32>) -> Vec<u32> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    #[tokio::test]
    async fn items_past_capacity_wait_in_order_until_there_is_room() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut outbox = Outbox::new(tx);
        for item in 1..=5 {
            outbox.send(item);
        }
        assert!(!outbox.is_empty());
        assert_eq!(drain(&mut rx), vec![1, 2]);

        outbox.ready().await;
        outbox.flush();
        outbox.send(6);
        assert_eq!(drain(&mut rx), vec![3, 4]);

        outbox.ready().await;
        outbox.flush();
        assert_eq!(drain(&mut rx), vec![5, 6]);
        assert!(outbox.is_empty());
    }

    #[tokio::test]
    async fn a_closed_receiver_empties_the_outbox() {
        let (tx, rx) = mpsc::channel(1);
        let mut outbox = Outbox::new(tx);
        outbox.send(1);
        outbox.send(2);
        drop(rx);

        outbox.ready().await;
        outbox.flush();

        assert!(outbox.is_empty());
        outbox.send(3);
        assert!(outbox.is_empty());
    }
}
