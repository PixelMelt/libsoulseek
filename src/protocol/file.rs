use super::wire::MessageWriter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileTransferInit {
    pub token: u32,
}

impl FileTransferInit {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = MessageWriter::new();
        w.write_u32(self.token);
        w.into_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOffset {
    pub offset: u64,
}

impl FileOffset {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = MessageWriter::new();
        w.write_u64(self.offset);
        w.into_bytes()
    }
}
