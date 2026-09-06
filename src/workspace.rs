use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::error::Error;
use crate::ids::{sha256_hex, BlobRef, SnapshotRev};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    pub exclude: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RestorePhase {
    Copying,
    Swapping,
    Done,
}

impl RestorePhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RestorePhase::Copying => "copying",
            RestorePhase::Swapping => "swapping",
            RestorePhase::Done => "done",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "copying" => Ok(RestorePhase::Copying),
            "swapping" => Ok(RestorePhase::Swapping),
            "done" => Ok(RestorePhase::Done),
            other => Err(Error::corrupt(format!("unknown restore phase {other}"))),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RestoreJournal {
    pub rev: SnapshotRev,
    pub phase: RestorePhase,
    pub scratch_path: PathBuf,
    pub bak_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Tree {
    pub v: u32,
    pub entries: Vec<TreeEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub blob: String,
    pub mode: u32,
    pub size: u64,
}

pub(crate) fn scratch_path(live: &Path) -> PathBuf {
    sibling_with_suffix(live, ".durable-scratch")
}

pub(crate) fn bak_path(live: &Path) -> PathBuf {
    sibling_with_suffix(live, ".durable-bak")
}

fn sibling_with_suffix(live: &Path, suffix: &str) -> PathBuf {
    let name = live
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "workspace".into());
    match live.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(format!("{name}{suffix}")),
        _ => PathBuf::from(format!("{name}{suffix}")),
    }
}

pub(crate) fn tree_bytes(tree: &Tree) -> Result<Vec<u8>, Error> {
    let mut entries = serde_json::Map::new();
    // Spec example lists `v` then `entries`; keep that field order so hashes are stable.
    let mut root = serde_json::Map::new();
    root.insert("v".into(), serde_json::json!(tree.v));
    let listed: Vec<serde_json::Value> = tree
        .entries
        .iter()
        .map(|e| {
            let mut m = serde_json::Map::new();
            m.insert("path".into(), serde_json::json!(e.path));
            m.insert("type".into(), serde_json::json!(e.kind));
            m.insert("blob".into(), serde_json::json!(e.blob));
            m.insert("mode".into(), serde_json::json!(e.mode));
            m.insert("size".into(), serde_json::json!(e.size));
            serde_json::Value::Object(m)
        })
        .collect();
    root.insert("entries".into(), serde_json::Value::Array(listed));
    let _ = entries;
    serde_json::to_vec(&serde_json::Value::Object(root)).map_err(|e| Error::store(e.to_string()))
}

pub(crate) fn parse_tree(bytes: &[u8]) -> Result<Tree, Error> {
    serde_json::from_slice(bytes).map_err(|e| Error::corrupt(format!("tree json: {e}")))
}

fn excluded_name(name: &str, extra: &[String]) -> bool {
    matches!(name, ".git" | "node_modules" | "target") || extra.iter().any(|e| e == name)
}

fn inside_store(path: &Path, store_dir: &Path) -> bool {
    let Ok(store) = store_dir.canonicalize() else {
        return false;
    };
    match path.canonicalize() {
        Ok(p) => p.starts_with(&store),
        Err(_) => path.starts_with(store_dir),
    }
}

pub(crate) fn capture(
    root: &Path,
    filter: &Filter,
    store_dir: &Path,
    mut put: impl FnMut(&[u8]) -> Result<BlobRef, Error>,
) -> Result<(BlobRef, Tree), Error> {
    let mut entries = Vec::new();
    if root.exists() {
        let walker = WalkDir::new(root).follow_links(false).into_iter().filter_entry(|e| {
            if e.depth() == 0 {
                return true;
            }
            let name = e.file_name().to_string_lossy();
            !excluded_name(&name, &filter.exclude)
        });
        for ent in walker {
            let ent = ent.map_err(|e| Error::Io(io_from_walkdir(e)))?;
            if ent.depth() == 0 {
                continue;
            }
            if inside_store(ent.path(), store_dir) {
                continue;
            }
            let ft = ent.file_type();
            if ft.is_dir() {
                continue;
            }
            let rel = ent
                .path()
                .strip_prefix(root)
                .map_err(|e| Error::invalid(e.to_string()))?;
            let rel_s = rel
                .to_str()
                .ok_or_else(|| Error::invalid("non-utf8 workspace path"))?
                .replace('\\', "/");
            if ft.is_symlink() {
                let target = fs::read_link(ent.path())?;
                let target_s = target
                    .to_str()
                    .ok_or_else(|| Error::invalid("non-utf8 symlink target"))?;
                let bytes = target_s.as_bytes();
                let blob = put(bytes)?;
                entries.push(TreeEntry {
                    path: rel_s,
                    kind: "symlink".into(),
                    blob: blob.uri(),
                    mode: 0o120777,
                    size: bytes.len() as u64,
                });
            } else if ft.is_file() {
                let bytes = fs::read(ent.path())?;
                let blob = put(&bytes)?;
                let mode = file_mode(ent.path());
                entries.push(TreeEntry {
                    path: rel_s,
                    kind: "file".into(),
                    blob: blob.uri(),
                    mode,
                    size: bytes.len() as u64,
                });
            }
        }
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let tree = Tree { v: 0, entries };
    let bytes = tree_bytes(&tree)?;
    let blob = put(&bytes)?;
    let expected = BlobRef::from_hex(sha256_hex(&bytes));
    if blob.as_hex() != expected.as_hex() {
        return Err(Error::corrupt("tree blob hash mismatch"));
    }
    Ok((blob, tree))
}

fn file_mode(path: &Path) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path)
            .map(|m| m.permissions().mode())
            .unwrap_or(0o100644)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        0o100644
    }
}

fn io_from_walkdir(err: walkdir::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
}

pub(crate) fn materialize(
    dest: &Path,
    tree: &Tree,
    mut get: impl FnMut(&BlobRef) -> Result<Vec<u8>, Error>,
) -> Result<(), Error> {
    if dest.exists() {
        fs::remove_dir_all(dest)?;
    }
    fs::create_dir_all(dest)?;
    for entry in &tree.entries {
        if entry.path.contains('\0') || Path::new(&entry.path).is_absolute() {
            return Err(Error::corrupt(format!("bad tree path {}", entry.path)));
        }
        let path = dest.join(&entry.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let blob = BlobRef::parse(&entry.blob)?;
        let bytes = get(&blob)?;
        if entry.kind == "symlink" {
            let target = String::from_utf8(bytes)
                .map_err(|_| Error::corrupt("symlink target is not utf-8"))?;
            #[cfg(unix)]
            {
                if path.exists() {
                    fs::remove_file(&path)?;
                }
                std::os::unix::fs::symlink(&target, &path)?;
            }
            #[cfg(not(unix))]
            {
                fs::write(&path, target.as_bytes())?;
            }
        } else {
            let mut f = File::create(&path)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = entry.mode & 0o777;
                fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

pub(crate) fn swap_live(live: &Path, scratch: &Path, bak: &Path) -> Result<(), Error> {
    if !bak.exists() && live.exists() {
        fs::rename(live, bak)?;
    }
    if scratch.exists() {
        fs::rename(scratch, live)?;
    } else if !live.exists() {
        return Err(Error::corrupt("restore swap missing scratch and live"));
    }
    Ok(())
}

pub(crate) fn cleanup_bak(bak: &Path) -> Result<(), Error> {
    if bak.exists() {
        if bak.is_dir() {
            fs::remove_dir_all(bak)?;
        } else {
            fs::remove_file(bak)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::sha256_hex;
    use std::collections::HashMap;

    #[test]
    fn workspace_restore_matches_hashes() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(src.join("sub")).unwrap();
        fs::write(src.join("README.md"), b"hello").unwrap();
        fs::write(src.join("sub/a.txt"), b"abc").unwrap();
        fs::create_dir_all(src.join("empty")).unwrap();
        fs::create_dir_all(src.join(".git")).unwrap();
        fs::write(src.join(".git/config"), b"nope").unwrap();

        let mut blobs = HashMap::new();
        let (tree_ref, tree) = {
            let mut put = |bytes: &[u8]| -> Result<BlobRef, Error> {
                let r = BlobRef::of_bytes(bytes);
                blobs.insert(r.as_hex().to_string(), bytes.to_vec());
                Ok(r)
            };
            capture(&src, &Filter::default(), &tmp.path().join("store"), &mut put).unwrap()
        };
        assert_eq!(tree_ref.as_hex(), sha256_hex(&tree_bytes(&tree).unwrap()));
        assert!(tree.entries.iter().all(|e| e.path != ".git/config"));
        assert!(tree.entries.iter().all(|e| !e.path.starts_with("empty")));

        let dest = tmp.path().join("dest");
        materialize(&dest, &tree, |b| {
            blobs
                .get(b.as_hex())
                .cloned()
                .ok_or_else(|| Error::corrupt("missing blob"))
        })
        .unwrap();

        let mut blobs2 = HashMap::new();
        let (tree_ref2, _) = {
            let mut put = |bytes: &[u8]| -> Result<BlobRef, Error> {
                let r = BlobRef::of_bytes(bytes);
                blobs2.insert(r.as_hex().to_string(), bytes.to_vec());
                Ok(r)
            };
            capture(&dest, &Filter::default(), &tmp.path().join("store"), &mut put).unwrap()
        };
        assert_eq!(tree_ref, tree_ref2);
        assert_eq!(fs::read(dest.join("README.md")).unwrap(), b"hello");
    }

    #[test]
    fn workspace_copying_crash_retries() {
        let tmp = tempfile::tempdir().unwrap();
        let live = tmp.path().join("live");
        fs::create_dir_all(&live).unwrap();
        fs::write(live.join("keep.txt"), b"old").unwrap();

        let mut blobs = HashMap::new();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("keep.txt"), b"new-tree").unwrap();
        let tree = {
            let mut put = |bytes: &[u8]| -> Result<BlobRef, Error> {
                let r = BlobRef::of_bytes(bytes);
                blobs.insert(r.as_hex().to_string(), bytes.to_vec());
                Ok(r)
            };
            capture(&src, &Filter::default(), &tmp.path().join("store"), &mut put)
                .unwrap()
                .1
        };

        let scratch = scratch_path(&live);
        fs::create_dir_all(&scratch).unwrap();
        fs::write(scratch.join("partial"), b"junk").unwrap();

        materialize(&scratch, &tree, |b| {
            blobs
                .get(b.as_hex())
                .cloned()
                .ok_or_else(|| Error::corrupt("missing blob"))
        })
        .unwrap();
        let bak = bak_path(&live);
        swap_live(&live, &scratch, &bak).unwrap();
        cleanup_bak(&bak).unwrap();
        assert_eq!(fs::read(live.join("keep.txt")).unwrap(), b"new-tree");
        assert!(!scratch.exists());
        assert!(!bak.exists());
    }
}
