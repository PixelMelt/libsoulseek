use std::ops::Range;

use crate::types::FileAttributes;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error(
        "virtual folder path {0:?} is invalid: its backslash-separated components must be \
         non-empty and contain no slash"
    )]
    InvalidFolder(String),
    #[error(
        "file name {name:?} in {folder:?} is invalid: names must be non-empty and contain \
         no slash or backslash"
    )]
    InvalidFileName { folder: String, name: String },
    #[error("file name {name:?} appears twice in {folder:?}")]
    DuplicateFile { folder: String, name: String },
    #[error("virtual folder path {0:?} appears twice")]
    DuplicateFolder(String),
}

#[derive(Debug, Clone, Default)]
pub struct ShareCatalog {
    folders: Vec<CatalogFolder>,
    files: Vec<CatalogFile>,
}

#[derive(Debug, Clone)]
pub struct CatalogFolder {
    virtual_path: Box<str>,
    virtual_path_lower: Box<str>,
    key: Box<[u8]>,
    files: Range<u32>,
    buddy_only: bool,
}

#[derive(Debug, Clone)]
pub struct CatalogFile {
    name: Box<str>,
    name_lower: Box<str>,
    key: Box<[u8]>,
    size: u64,
    modified: u64,
    attributes: FileAttributes,
}

fn valid_folder(virtual_path: &str) -> bool {
    virtual_path
        .split('\\')
        .all(|component| !component.is_empty() && !component.contains('/'))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['/', '\\'])
}

impl ShareCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_folder(
        &mut self,
        virtual_path: String,
        key: Vec<u8>,
        buddy_only: bool,
        mut files: Vec<CatalogFile>,
    ) -> Result<(), CatalogError> {
        if !valid_folder(&virtual_path) {
            return Err(CatalogError::InvalidFolder(virtual_path));
        }
        if let Some(file) = files.iter().find(|file| !valid_name(&file.name)) {
            return Err(CatalogError::InvalidFileName {
                folder: virtual_path,
                name: file.name.to_string(),
            });
        }
        files.sort_by(|left, right| left.name.cmp(&right.name));
        if let Some(pair) = files.windows(2).find(|pair| pair[0].name == pair[1].name) {
            return Err(CatalogError::DuplicateFile {
                folder: virtual_path,
                name: pair[0].name.to_string(),
            });
        }
        let start = self.files.len() as u32;
        self.files.extend(files);
        self.folders.push(CatalogFolder {
            virtual_path_lower: virtual_path.to_lowercase().into_boxed_str(),
            virtual_path: virtual_path.into_boxed_str(),
            key: key.into_boxed_slice(),
            files: start..self.files.len() as u32,
            buddy_only,
        });
        Ok(())
    }

    pub fn folders(&self) -> &[CatalogFolder] {
        &self.folders
    }

    pub fn files(&self, folder: &CatalogFolder) -> &[CatalogFile] {
        &self.files[folder.files.start as usize..folder.files.end as usize]
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub(crate) fn all_files(&self) -> &[CatalogFile] {
        &self.files
    }

    pub(crate) fn set_attributes(&mut self, file: u32, attributes: FileAttributes) {
        self.files[file as usize].attributes = attributes;
    }
}

impl CatalogFolder {
    pub fn virtual_path(&self) -> &str {
        &self.virtual_path
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn buddy_only(&self) -> bool {
        self.buddy_only
    }

    pub(crate) fn virtual_path_lower(&self) -> &str {
        &self.virtual_path_lower
    }

    pub(crate) fn file_range(&self) -> Range<u32> {
        self.files.clone()
    }
}

impl CatalogFile {
    pub fn new(
        name: String,
        key: Vec<u8>,
        size: u64,
        modified: u64,
        attributes: FileAttributes,
    ) -> Self {
        Self {
            name_lower: name.to_lowercase().into_boxed_str(),
            name: name.into_boxed_str(),
            key: key.into_boxed_slice(),
            size,
            modified,
            attributes,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn modified(&self) -> u64 {
        self.modified
    }

    pub fn attributes(&self) -> &FileAttributes {
        &self.attributes
    }

    pub(crate) fn name_lower(&self) -> &str {
        &self.name_lower
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str) -> CatalogFile {
        CatalogFile::new(
            name.into(),
            name.as_bytes().to_vec(),
            1,
            0,
            FileAttributes::default(),
        )
    }

    #[test]
    fn folders_and_names_follow_the_virtual_path_rules() {
        let mut catalog = ShareCatalog::new();
        for path in ["", "Music\\", "\\Music", "Mu/sic", "Music\\\\Album"] {
            assert_eq!(
                catalog.push_folder(path.into(), Vec::new(), false, Vec::new()),
                Err(CatalogError::InvalidFolder(path.into()))
            );
        }
        for name in ["", "a/b", "a\\b"] {
            assert!(matches!(
                catalog.push_folder("Music".into(), Vec::new(), false, vec![file(name)]),
                Err(CatalogError::InvalidFileName { .. })
            ));
        }
        assert!(matches!(
            catalog.push_folder(
                "Music".into(),
                Vec::new(),
                false,
                vec![file("b.mp3"), file("a.mp3"), file("b.mp3")]
            ),
            Err(CatalogError::DuplicateFile { .. })
        ));
        assert!(catalog.folders().is_empty());
        catalog
            .push_folder(
                "Music\\Album".into(),
                b"/music/album".to_vec(),
                true,
                vec![file("b.mp3"), file("a.mp3")],
            )
            .unwrap();
        let folder = &catalog.folders()[0];
        let names: Vec<&str> = catalog
            .files(folder)
            .iter()
            .map(CatalogFile::name)
            .collect();
        assert_eq!(names, ["a.mp3", "b.mp3"]);
        assert!(folder.buddy_only());
    }
}
