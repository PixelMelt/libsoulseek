use super::{Download, DownloadRequest};

pub const DEFAULT_BASENAME_BYTE_LIMIT: usize = 255;

fn clean_file_name(name: &str) -> String {
    let replaced: String = name
        .chars()
        .map(|c| {
            if matches!(
                c,
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\u{0}'..='\u{1f}'
            ) {
                '_'
            } else {
                c
            }
        })
        .collect();
    let stripped = replaced
        .trim_start_matches(' ')
        .trim_end_matches(['.', ' ']);
    if stripped.is_empty() {
        return "_".repeat(replaced.chars().count());
    }
    stripped.to_owned()
}

fn virtual_folder(virtual_path: &str) -> &str {
    virtual_path
        .rsplit_once('\\')
        .map_or("", |(folder, _)| folder)
}

fn virtual_basename(virtual_path: &str) -> &str {
    virtual_path
        .rsplit_once('\\')
        .map_or(virtual_path, |(_, basename)| basename)
}

pub fn download_folder(request: &DownloadRequest<'_>, username_subfolders: bool) -> Vec<String> {
    let mut parts = Vec::new();
    if username_subfolders {
        parts.push(request.username);
    }
    if let Some(root) = request.root {
        let removed_parents = root.rsplit_once('\\').map_or("", |(parent, _)| parent);
        let folder = virtual_folder(request.virtual_path);
        let target = folder.strip_prefix(removed_parents).unwrap_or(folder);
        parts.extend(target.split('\\'));
    }
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .map(clean_file_name)
        .collect()
}

pub fn download_basename(virtual_path: &str, max_bytes: usize) -> String {
    let basename = clean_file_name(virtual_basename(virtual_path));
    let (stem, extension) = split_extension(&basename);
    if extension.len() > max_bytes {
        return extension[..extension.floor_char_boundary(max_bytes)].to_owned();
    }
    format!(
        "{}{extension}",
        &stem[..stem.floor_char_boundary(max_bytes - extension.len())]
    )
}

pub fn incomplete_basename(download: &Download, max_bytes: usize) -> String {
    let digest = md5::compute(format!("{}{}", download.virtual_path, download.username).as_bytes());
    let prefix = format!("INCOMPLETE{digest:x}");
    let basename = download_basename(
        &download.virtual_path,
        max_bytes.saturating_sub(prefix.len()),
    );
    format!("{prefix}{basename}")
}

pub fn candidate_basenames(basename: &str) -> impl Iterator<Item = String> + '_ {
    let (stem, extension) = split_extension(basename);
    let numbered = (1..).map(move |counter| format!("{stem} ({counter}){extension}"));
    std::iter::once(basename.to_owned()).chain(numbered)
}

fn split_extension(basename: &str) -> (&str, &str) {
    match basename.rfind('.') {
        Some(index) if index > 0 => basename.split_at(index),
        _ => (basename, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn destination(username_subfolders: bool, virtual_path: &str, root: Option<&str>) -> String {
        let request = DownloadRequest {
            username: "newuser",
            virtual_path,
            root,
        };
        let mut parts = vec!["/data".to_owned()];
        parts.extend(download_folder(&request, username_subfolders));
        parts.join("/")
    }

    #[test]
    fn folder_destination_matches_nicotine() {
        assert_eq!(
            destination(false, "Hello\\Path\\file.mp3", Some("Hello\\Path")),
            "/data/Path"
        );
        assert_eq!(
            destination(true, "Hello\\Path\\file.mp3", Some("Hello\\Path")),
            "/data/newuser/Path"
        );
        assert_eq!(
            destination(true, "Hello\\file.mp3", Some("Hello")),
            "/data/newuser/Hello"
        );
        assert_eq!(
            destination(
                true,
                "Hello\\Path\\Depth\\Hello Depth Test Path\\file.mp3",
                Some("Hello\\Path\\Depth\\Hello Depth Test Path")
            ),
            "/data/newuser/Hello Depth Test Path"
        );
    }

    #[test]
    fn folder_destination_keeps_subfolders_below_the_root() {
        let root = Some("share\\Soulseek");
        assert_eq!(
            destination(false, "share\\Soulseek\\file1.mp3", root),
            "/data/Soulseek"
        );
        assert_eq!(
            destination(false, "share\\Soulseek\\folder1\\file3.mp3", root),
            "/data/Soulseek/folder1"
        );
        assert_eq!(
            destination(false, "share\\Soulseek\\folder2\\sub2\\file6.mp3", root),
            "/data/Soulseek/folder2/sub2"
        );
        assert_eq!(
            destination(false, "share\\file.mp3", Some("share")),
            "/data/share"
        );
        assert_eq!(
            destination(
                false,
                "share\\Soulseek\\folder1\\sub1\\file4.mp3",
                Some("share")
            ),
            "/data/share/Soulseek/folder1/sub1"
        );
    }

    #[test]
    fn single_file_downloads_land_in_the_default_folder() {
        assert_eq!(destination(false, "share\\Music\\song.mp3", None), "/data");
        assert_eq!(
            destination(true, "share\\Music\\song.mp3", None),
            "/data/newuser"
        );
    }

    #[test]
    fn clean_file_name_matches_nicotine() {
        assert_eq!(clean_file_name(".."), "__");
        assert_eq!(
            clean_file_name("a/b\\c:d*e?f\"g<h>i|j"),
            "a_b_c_d_e_f_g_h_i_j"
        );
        assert_eq!(clean_file_name("song\u{7}name\u{1f}.mp3"), "song_name_.mp3");
        assert_eq!(clean_file_name("  spaced out. . "), "spaced out");
        assert_eq!(clean_file_name("///"), "___");
        assert_eq!(clean_file_name("片片片"), "片片片");
    }

    #[test]
    fn download_basename_stays_within_the_byte_limit() {
        let virtual_path = format!("Music\\{}.mp3", "片".repeat(200));
        let basename = download_basename(&virtual_path, 255);
        assert!(basename.len() <= 255);
        assert!(basename.starts_with('片'));
        assert!(basename.ends_with(".mp3"));

        let long_extension = format!("Music\\name.{}", "x".repeat(300));
        assert_eq!(download_basename(&long_extension, 8).len(), 8);
    }

    #[test]
    fn incomplete_names_are_prefixed_with_the_download_digest() {
        let download = Download {
            username: "peer".into(),
            virtual_path: "share\\Album\\song.mp3".into(),
            size: 6,
            destination: "/data".into(),
        };
        let prefix = "INCOMPLETE3b547388b3820c2c370aa9c94452674d";
        assert_eq!(
            incomplete_basename(&download, DEFAULT_BASENAME_BYTE_LIMIT),
            format!("{prefix}song.mp3")
        );
        assert_eq!(
            incomplete_basename(&download, prefix.len() + 4),
            format!("{prefix}.mp3")
        );
    }

    #[test]
    fn collision_candidates_number_the_stem() {
        let first: Vec<String> = candidate_basenames("song.mp3").take(3).collect();
        assert_eq!(first, ["song.mp3", "song (1).mp3", "song (2).mp3"]);
        let hidden: Vec<String> = candidate_basenames(".hidden").take(2).collect();
        assert_eq!(hidden, [".hidden", ".hidden (1)"]);
    }
}
