mod wire;

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use super::punctuation::split_words;
use crate::storage::{CatalogError, CatalogFolder, ShareCatalog, ShareKey, ShareSource};
use crate::types::{FileAttributes, FileInfo, FolderContents};

pub fn empty_browse_frame() -> Vec<u8> {
    wire::encode_shared_file_list(&ShareCatalog::new(), &[], false)
}

#[derive(Debug, Default)]
struct WordPostings {
    files: Vec<u32>,
    folders: Vec<u32>,
}

pub struct SharesIndex {
    catalog: Arc<ShareCatalog>,
    source: Arc<dyn ShareSource>,
    folders_by_path: Vec<u32>,
    folders_by_lower_path: Vec<u32>,
    resolve_files: Vec<u32>,
    word_ids: HashMap<Box<str>, u32>,
    postings: Vec<WordPostings>,
    suffixes: Vec<(Box<str>, u32)>,
    public_counts: (u32, u32),
    public_browse_frame: Vec<u8>,
    buddy_browse_frame: Option<Vec<u8>>,
}

fn index_words(
    word_index: &mut HashMap<Box<str>, WordPostings>,
    text: &str,
    mut post: impl FnMut(&mut WordPostings),
) {
    let mut words: Vec<&str> = split_words(text).collect();
    words.sort_unstable();
    words.dedup();
    for word in words {
        match word_index.get_mut(word) {
            Some(postings) => post(postings),
            None => {
                let mut postings = WordPostings::default();
                post(&mut postings);
                word_index.insert(word.into(), postings);
            }
        }
    }
}

impl SharesIndex {
    pub fn from_catalog(
        catalog: Arc<ShareCatalog>,
        source: Arc<dyn ShareSource>,
    ) -> Result<Self, CatalogError> {
        let folders = catalog.folders();
        let files = catalog.all_files();
        let mut folders_by_path: Vec<u32> = (0..folders.len() as u32).collect();
        folders_by_path.sort_by(|&left, &right| {
            folders[left as usize]
                .virtual_path()
                .cmp(folders[right as usize].virtual_path())
        });
        if let Some(pair) = folders_by_path.windows(2).find(|pair| {
            folders[pair[0] as usize].virtual_path() == folders[pair[1] as usize].virtual_path()
        }) {
            return Err(CatalogError::DuplicateFolder(
                folders[pair[0] as usize].virtual_path().to_owned(),
            ));
        }

        let mut word_index: HashMap<Box<str>, WordPostings> = HashMap::new();
        for (folder_id, folder) in folders.iter().enumerate() {
            if !folder.file_range().is_empty() {
                index_words(&mut word_index, folder.virtual_path_lower(), |postings| {
                    postings.folders.push(folder_id as u32)
                });
            }
            for file_id in folder.file_range() {
                index_words(
                    &mut word_index,
                    files[file_id as usize].name_lower(),
                    |postings| postings.files.push(file_id),
                );
            }
        }
        let mut folders_by_lower_path = folders_by_path.clone();
        folders_by_lower_path.sort_by(|&left, &right| {
            folders[left as usize]
                .virtual_path_lower()
                .cmp(folders[right as usize].virtual_path_lower())
                .then_with(|| left.cmp(&right))
        });
        let mut resolve_files = Vec::with_capacity(files.len());
        for folder in folders {
            let start = resolve_files.len();
            resolve_files.extend(folder.file_range());
            resolve_files[start..].sort_by(|&left, &right| {
                files[left as usize]
                    .name_lower()
                    .cmp(files[right as usize].name_lower())
                    .then_with(|| left.cmp(&right))
            });
        }

        let mut word_ids = HashMap::with_capacity(word_index.len());
        let mut postings = Vec::with_capacity(word_index.len());
        let mut suffixes = Vec::with_capacity(word_index.len());
        for (word, word_postings) in word_index {
            let id = postings.len() as u32;
            suffixes.push((word.chars().rev().collect::<Box<str>>(), id));
            word_ids.insert(word, id);
            postings.push(word_postings);
        }
        suffixes.sort_unstable();

        let public_folders = folders.iter().filter(|folder| !folder.buddy_only()).count();
        let public_files = folders
            .iter()
            .filter(|folder| !folder.buddy_only())
            .map(|folder| folder.file_range().len())
            .sum::<usize>();
        let has_buddy_folders = public_folders < folders.len();
        let public_browse_frame = wire::encode_shared_file_list(&catalog, &folders_by_path, false);
        let buddy_browse_frame = has_buddy_folders
            .then(|| wire::encode_shared_file_list(&catalog, &folders_by_path, true));

        Ok(Self {
            catalog,
            source,
            folders_by_path,
            folders_by_lower_path,
            resolve_files,
            word_ids,
            postings,
            suffixes,
            public_counts: (public_folders as u32, public_files as u32),
            public_browse_frame,
            buddy_browse_frame,
        })
    }

    pub fn source(&self) -> &Arc<dyn ShareSource> {
        &self.source
    }

    pub fn counts(&self) -> (u32, u32) {
        self.public_counts
    }

    pub fn browse_frame(&self, is_buddy: bool) -> &[u8] {
        match (&self.buddy_browse_frame, is_buddy) {
            (Some(frame), true) => frame,
            _ => &self.public_browse_frame,
        }
    }

    #[cfg(test)]
    pub fn browse(&self, is_buddy: bool) -> Vec<FolderContents> {
        self.folders_by_path
            .iter()
            .map(|&folder| &self.catalog.folders()[folder as usize])
            .filter(|folder| is_buddy || !folder.buddy_only())
            .map(|folder| self.folder_view(folder))
            .collect()
    }

    pub fn folder_contents(&self, directory: &str, is_buddy: bool) -> Vec<FolderContents> {
        let folders = self.catalog.folders();
        let Ok(position) = self
            .folders_by_path
            .binary_search_by(|&folder| folders[folder as usize].virtual_path().cmp(directory))
        else {
            return Vec::new();
        };
        let folder = &folders[self.folders_by_path[position] as usize];
        if folder.buddy_only() && !is_buddy {
            return Vec::new();
        }
        vec![self.folder_view(folder)]
    }

    pub fn resolve(
        &self,
        virtual_path: &str,
        is_buddy: bool,
    ) -> Option<(ShareKey, u64, &FileAttributes)> {
        let folders = self.catalog.folders();
        let files = self.catalog.all_files();
        let (directory, name) = virtual_path.rsplit_once('\\')?;
        let directory_lower = directory.to_lowercase();
        let name_lower = name.to_lowercase();
        let folder_range = equal_range(&self.folders_by_lower_path, |&folder| {
            folders[folder as usize]
                .virtual_path_lower()
                .cmp(directory_lower.as_str())
        });
        let mut first = None;
        let mut exact = None;
        for &folder_id in &self.folders_by_lower_path[folder_range] {
            let folder = &folders[folder_id as usize];
            let range = folder.file_range();
            let candidates = &self.resolve_files[range.start as usize..range.end as usize];
            let file_range = equal_range(candidates, |&file| {
                files[file as usize].name_lower().cmp(name_lower.as_str())
            });
            for &file_id in &candidates[file_range] {
                first.get_or_insert((folder_id, file_id));
                let file = &files[file_id as usize];
                if folder.virtual_path() == directory && file.name() == name {
                    exact = Some((folder_id, file_id));
                    break;
                }
            }
            if exact.is_some() {
                break;
            }
        }
        let (folder_id, file_id) = exact.or(first)?;
        let folder = &folders[folder_id as usize];
        if folder.buddy_only() && !is_buddy {
            return None;
        }
        let file = &files[file_id as usize];
        Some((
            ShareKey::new(folder.key(), file.key()),
            file.size(),
            file.attributes(),
        ))
    }

    pub fn search(
        &self,
        search_term: &str,
        is_buddy: bool,
        excluded_phrases: &[String],
        max_results: usize,
        min_chars: usize,
    ) -> Vec<FileInfo> {
        if search_term.chars().count() < min_chars {
            return Vec::new();
        }
        let term_lower = search_term.to_lowercase();
        let mut excluded_words = HashSet::new();
        let mut partial_words = HashSet::new();
        if term_lower.contains('-') || term_lower.contains('*') {
            for word in term_lower.split_whitespace() {
                if let Some(rest) = word.strip_prefix('-') {
                    excluded_words.extend(split_words(rest));
                } else if let Some(rest) = word.strip_prefix('*') {
                    partial_words.extend(split_words(rest));
                }
            }
        }
        let mut included = Vec::new();
        for word in split_words(&term_lower).collect::<HashSet<&str>>() {
            if excluded_words.contains(word) || partial_words.contains(word) {
                continue;
            }
            let Some(postings) = self.postings(word) else {
                return Vec::new();
            };
            included.push(postings);
        }
        included.sort_by_key(|postings| self.posting_len(postings));
        let Some((seed, rest)) = included.split_first() else {
            return Vec::new();
        };
        let partial: Vec<HashSet<u32>> = partial_words
            .iter()
            .map(|suffix| self.suffix_files(suffix))
            .collect();
        if partial.iter().any(HashSet::is_empty) {
            return Vec::new();
        }
        let excluded: Vec<&WordPostings> = excluded_words
            .iter()
            .filter_map(|word| self.postings(word))
            .collect();

        let folders = self.catalog.folders();
        let files = self.catalog.all_files();
        let mut results = Vec::new();
        let mut path_lower = String::new();
        for file_id in self.posting_files(seed) {
            if !rest
                .iter()
                .all(|postings| self.posting_contains(postings, file_id))
                || !partial.iter().all(|files| files.contains(&file_id))
                || excluded
                    .iter()
                    .any(|postings| self.posting_contains(postings, file_id))
            {
                continue;
            }
            let folder = &folders[self.folder_for_file(file_id) as usize];
            if !is_buddy && folder.buddy_only() {
                continue;
            }
            let file = &files[file_id as usize];
            if !excluded_phrases.is_empty() {
                path_lower.clear();
                path_lower.push_str(folder.virtual_path_lower());
                path_lower.push('\\');
                path_lower.push_str(file.name_lower());
                if excluded_phrases
                    .iter()
                    .any(|phrase| path_lower.contains(phrase.as_str()))
                {
                    continue;
                }
            }
            results.push(FileInfo {
                name: format!("{}\\{}", folder.virtual_path(), file.name()),
                size: file.size(),
                attributes: file.attributes().clone(),
            });
            if results.len() == max_results {
                break;
            }
        }
        results.sort_by(|left, right| left.name.cmp(&right.name));
        results
    }

    fn postings(&self, word: &str) -> Option<&WordPostings> {
        self.word_ids
            .get(word)
            .map(|&id| &self.postings[id as usize])
    }

    fn posting_len(&self, postings: &WordPostings) -> usize {
        let folders = self.catalog.folders();
        postings.files.len()
            + postings
                .folders
                .iter()
                .map(|&folder| folders[folder as usize].file_range().len())
                .sum::<usize>()
    }

    fn posting_contains(&self, postings: &WordPostings, file_id: u32) -> bool {
        postings.files.binary_search(&file_id).is_ok()
            || postings
                .folders
                .binary_search(&self.folder_for_file(file_id))
                .is_ok()
    }

    fn posting_files<'a>(&'a self, postings: &'a WordPostings) -> impl Iterator<Item = u32> + 'a {
        let folders = self.catalog.folders();
        let mut next_file = 0usize;
        let mut next_folder = 0usize;
        let mut folder_files: Option<Range<u32>> = None;
        std::iter::from_fn(move || {
            if folder_files.as_ref().is_none_or(Range::is_empty) {
                folder_files = postings.folders.get(next_folder).map(|&folder| {
                    next_folder += 1;
                    folders[folder as usize].file_range()
                });
            }
            let from_file = postings.files.get(next_file).copied();
            let from_folder = folder_files.as_ref().map(|range| range.start);
            match (from_file, from_folder) {
                (None, None) => None,
                (Some(file), None) => {
                    next_file += 1;
                    Some(file)
                }
                (None, Some(file)) => {
                    folder_files.as_mut().unwrap().start += 1;
                    Some(file)
                }
                (Some(file), Some(folder_file)) => {
                    if file <= folder_file {
                        next_file += 1;
                    }
                    if folder_file <= file {
                        folder_files.as_mut().unwrap().start += 1;
                    }
                    Some(file.min(folder_file))
                }
            }
        })
    }

    fn suffix_files(&self, suffix: &str) -> HashSet<u32> {
        let key: String = suffix.chars().rev().collect();
        let start = self
            .suffixes
            .partition_point(|(word, _)| word.as_ref() < key.as_str());
        let end = start
            + self.suffixes[start..].partition_point(|(word, _)| word.starts_with(key.as_str()));
        let mut files = HashSet::new();
        for &(_, id) in &self.suffixes[start..end] {
            files.extend(self.posting_files(&self.postings[id as usize]));
        }
        files
    }

    fn folder_view(&self, folder: &CatalogFolder) -> FolderContents {
        FolderContents {
            directory: folder.virtual_path().to_owned(),
            files: self
                .catalog
                .files(folder)
                .iter()
                .map(|file| FileInfo {
                    name: file.name().to_owned(),
                    size: file.size(),
                    attributes: file.attributes().clone(),
                })
                .collect(),
        }
    }

    fn folder_for_file(&self, file_id: u32) -> u32 {
        self.catalog
            .folders()
            .partition_point(|folder| folder.file_range().end <= file_id) as u32
    }
}

fn equal_range<T>(slice: &[T], mut compare: impl FnMut(&T) -> std::cmp::Ordering) -> Range<usize> {
    let start = slice.partition_point(|item| compare(item).is_lt());
    let end = start + slice[start..].partition_point(|item| compare(item).is_eq());
    start..end
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io;

    use super::*;
    use crate::storage::{CatalogFile, OpenedShare, ScanError, ScanProgress};
    use crate::types::{BoxFuture, DEFAULT_MAX_SEARCH_RESULTS, DEFAULT_MIN_SEARCH_CHARS};

    pub(crate) struct NoSource;

    impl ShareSource for NoSource {
        fn load(&self) -> BoxFuture<Result<Option<ShareCatalog>, ScanError>> {
            unreachable!("index tests never load")
        }

        fn scan(&self, _: ScanProgress) -> BoxFuture<Result<ShareCatalog, ScanError>> {
            unreachable!("index tests never scan")
        }

        fn size(&self, _: ShareKey) -> BoxFuture<io::Result<u64>> {
            unreachable!("index tests never read files")
        }

        fn open(&self, _: ShareKey, _: u64) -> BoxFuture<io::Result<OpenedShare>> {
            unreachable!("index tests never read files")
        }
    }

    pub(crate) fn file(name: &str, size: u64) -> CatalogFile {
        CatalogFile::new(
            name.into(),
            name.as_bytes().to_vec(),
            size,
            0,
            FileAttributes::default(),
        )
    }

    pub(crate) fn index_of(folders: Vec<(&str, bool, Vec<CatalogFile>)>) -> SharesIndex {
        let mut catalog = ShareCatalog::new();
        for (virtual_path, buddy_only, files) in folders {
            catalog
                .push_folder(
                    virtual_path.into(),
                    format!("/{}", virtual_path.replace('\\', "/")).into_bytes(),
                    buddy_only,
                    files,
                )
                .unwrap();
        }
        SharesIndex::from_catalog(Arc::new(catalog), Arc::new(NoSource)).unwrap()
    }

    fn index(names: &[&str]) -> SharesIndex {
        index_of(vec![(
            "Music\\Caf\u{e9}",
            false,
            names.iter().map(|name| file(name, 1)).collect(),
        )])
    }

    fn sample_index() -> SharesIndex {
        index_of(vec![
            ("Public", false, Vec::new()),
            (
                "Public\\Sample Album",
                false,
                vec![file("First Song.flac", 300), file("Second Tune.ogg", 300)],
            ),
            ("Private", true, vec![file("hidden song.wav", 300)]),
        ])
    }

    fn search(
        index: &SharesIndex,
        term: &str,
        is_buddy: bool,
        phrases: &[String],
    ) -> Vec<FileInfo> {
        index.search(
            term,
            is_buddy,
            phrases,
            DEFAULT_MAX_SEARCH_RESULTS,
            DEFAULT_MIN_SEARCH_CHARS,
        )
    }

    fn names(results: Vec<FileInfo>) -> Vec<String> {
        results.into_iter().map(|file| file.name).collect()
    }

    #[test]
    fn minimum_length_counts_characters_not_bytes() {
        let index = index(&["\u{e4}\u{f6}.mp3"]);
        assert_eq!(index.search("\u{e4}\u{f6}", false, &[], 10, 3), Vec::new());
        assert_eq!(
            names(index.search("\u{e4}\u{f6}", false, &[], 10, 2)),
            ["Music\\Caf\u{e9}\\\u{e4}\u{f6}.mp3"]
        );
    }

    #[test]
    fn index_and_queries_split_on_the_nicotine_punctuation_set() {
        let index = index(&[
            "\u{2665}love.mp3",
            "live\u{2013}set.mp3",
            "caf\u{e9}_tunes.mp3",
        ]);
        assert!(index.search("love", false, &[], 10, 1).is_empty());
        assert_eq!(
            names(index.search("\u{2665}love", false, &[], 10, 1)),
            ["Music\\Caf\u{e9}\\\u{2665}love.mp3"]
        );
        assert_eq!(
            names(index.search("set live", false, &[], 10, 1)),
            ["Music\\Caf\u{e9}\\live\u{2013}set.mp3"]
        );
        assert_eq!(
            names(index.search("caf\u{e9}\u{2014}tunes", false, &[], 10, 1)),
            ["Music\\Caf\u{e9}\\caf\u{e9}_tunes.mp3"]
        );
    }

    #[test]
    fn search_word_matching() {
        let index = sample_index();

        let results = search(&index, "sample first", false, &[]);
        assert_eq!(names(results), ["Public\\Sample Album\\First Song.flac"]);
        assert!(search(&index, "sample missing", false, &[]).is_empty());
        assert_eq!(
            names(search(&index, "sample -tune", false, &[])),
            ["Public\\Sample Album\\First Song.flac"]
        );
        assert_eq!(
            names(search(&index, "sample *une", false, &[])),
            ["Public\\Sample Album\\Second Tune.ogg"]
        );
        assert_eq!(search(&index, "SAMPLE ALBUM", false, &[]).len(), 2);
        assert!(search(&index, "ab", false, &[]).is_empty());
    }

    #[test]
    fn search_respects_permissions_and_phrases() {
        let index = sample_index();

        assert!(search(&index, "hidden song", false, &[]).is_empty());
        assert_eq!(
            names(search(&index, "hidden song", true, &[])),
            ["Private\\hidden song.wav"]
        );
        assert!(search(&index, "first song", false, &["first".into()]).is_empty());
    }

    #[test]
    fn case_colliding_paths_resolve_exact_case_first() {
        let index = index_of(vec![
            ("Music", false, Vec::new()),
            (
                "Music\\moe shop - pure",
                false,
                vec![file("Crush.mp3", 200)],
            ),
            (
                "Music\\Moe Shop - Pure",
                false,
                vec![file("Crush.mp3", 300)],
            ),
        ]);

        let (key, size, _) = index
            .resolve("Music\\Moe Shop - Pure\\Crush.mp3", false)
            .unwrap();
        assert_eq!(size, 300);
        assert_eq!(
            key,
            ShareKey::new(&b"/Music/Moe Shop - Pure"[..], &b"Crush.mp3"[..])
        );
        let (_, size, _) = index
            .resolve("Music\\moe shop - pure\\Crush.mp3", false)
            .unwrap();
        assert_eq!(size, 200);
        assert!(
            index
                .resolve("MUSIC\\MOE SHOP - PURE\\CRUSH.MP3", false)
                .is_some()
        );
    }

    #[test]
    fn browse_and_resolve() {
        let index = sample_index();

        assert!(
            index
                .browse(false)
                .iter()
                .all(|folder| !folder.directory.starts_with("Private"))
        );
        assert!(
            index
                .browse(true)
                .iter()
                .any(|folder| folder.directory == "Private")
        );
        let (_, size, _) = index
            .resolve("public\\sample album\\FIRST SONG.FLAC", false)
            .expect("case-insensitive resolve");
        assert_eq!(size, 300);
        assert!(index.resolve("Private\\hidden song.wav", false).is_none());
        assert!(index.resolve("Private\\hidden song.wav", true).is_some());
        assert_eq!(
            index.folder_contents("Public\\Sample Album", false).len(),
            1
        );
        assert!(index.folder_contents("Private", false).is_empty());
        assert!(index.folder_contents("Nope", true).is_empty());
    }

    #[test]
    fn streaming_browse_matches_peer_message_encoding() {
        let index = sample_index();
        for is_buddy in [false, true] {
            let expected = crate::protocol::PeerMessage::SharedFileListResponse {
                shares: index.browse(is_buddy),
                unknown: 0,
                private_shares: Vec::new(),
            };
            let bytes = index.browse_frame(is_buddy);
            assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 5);
            let parsed = crate::protocol::PeerMessage::parse(5, &bytes[8..]).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn search_skips_trailing_empty_folder() {
        let index = index_of(vec![
            ("Music", false, vec![file("song.flac", 300)]),
            ("Music\\Albums", false, Vec::new()),
        ]);

        assert_eq!(
            names(search(&index, "music", false, &[])),
            vec!["Music\\song.flac"]
        );
    }

    #[test]
    fn search_merges_folder_and_file_postings() {
        let index = index_of(vec![
            ("Music", false, vec![file("Sample Loose.mp3", 300)]),
            (
                "Music\\Sample",
                false,
                vec![file("Sample.flac", 300), file("Other.flac", 300)],
            ),
        ]);

        assert_eq!(
            names(search(&index, "sample", false, &[])),
            vec![
                "Music\\Sample Loose.mp3",
                "Music\\Sample\\Other.flac",
                "Music\\Sample\\Sample.flac",
            ]
        );
        assert_eq!(
            names(search(&index, "sample other", false, &[])),
            vec!["Music\\Sample\\Other.flac"]
        );
        assert_eq!(
            names(search(&index, "sample -other", false, &[])),
            vec!["Music\\Sample Loose.mp3", "Music\\Sample\\Sample.flac"]
        );
        assert_eq!(
            names(search(&index, "sample *ose", false, &[])),
            vec!["Music\\Sample Loose.mp3"]
        );
        assert_eq!(
            names(search(&index, "sample", false, &["sample\\other".into()])),
            vec!["Music\\Sample Loose.mp3", "Music\\Sample\\Sample.flac"]
        );
        assert_eq!(index.search("sample", false, &[], 2, 1).len(), 2);
    }

    #[test]
    fn duplicate_virtual_folders_are_refused() {
        let mut catalog = ShareCatalog::new();
        for _ in 0..2 {
            catalog
                .push_folder("Music".into(), Vec::new(), false, Vec::new())
                .unwrap();
        }
        assert!(matches!(
            SharesIndex::from_catalog(Arc::new(catalog), Arc::new(NoSource)),
            Err(CatalogError::DuplicateFolder(path)) if path == "Music"
        ));
    }
}
