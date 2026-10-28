//! Persistence — Store trait + FileStore implementation.
//!
//! Mirrors the Go Store interface: save/load/delete/list nodes,
//! plus user records and server key for the transport layer.
//! FileStore writes JSON files with atomic rename.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Store trait
// ---------------------------------------------------------------------------

/// Persistence interface for mimesis state.
pub trait Store: Send + Sync {
    /// Save a node's serialized state.
    fn save_node(&self, id: &str, data: &[u8]) -> io::Result<()>;
    /// Load a node's raw JSON. Returns None if not found.
    fn load_node(&self, id: &str) -> io::Result<Option<Vec<u8>>>;
    /// Delete a node's persisted state.
    fn delete_node(&self, id: &str) -> io::Result<()>;
    /// List all persisted node IDs.
    fn list_nodes(&self) -> io::Result<Vec<String>>;
    /// Save user records.
    fn save_users(&self, users: &HashMap<String, UserRecord>) -> io::Result<()>;
    /// Load user records.
    fn load_users(&self) -> io::Result<HashMap<String, UserRecord>>;
    /// Save the server's ed25519 seed.
    fn save_server_key(&self, seed: &[u8]) -> io::Result<()>;
    /// Load the server's ed25519 seed. Returns None if not found.
    fn load_server_key(&self) -> io::Result<Option<Vec<u8>>>;
}

/// A persisted user account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub name: String,
    #[serde(with = "hex_bytes")]
    pub salt: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub hash: Vec<u8>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub admin: bool,
}

fn is_false(v: &bool) -> bool {
    !*v
}

// ---------------------------------------------------------------------------
// FileStore
// ---------------------------------------------------------------------------

/// File-based Store implementation using atomic writes.
pub struct FileStore {
    dir: PathBuf,
}

impl FileStore {
    /// Create a new FileStore, ensuring the nodes directory exists.
    pub fn new(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(dir.join("nodes"))?;
        Ok(FileStore { dir })
    }

    fn node_path(&self, id: &str) -> PathBuf {
        let safe = id
            .replace('@', "_at_")
            .replace(':', "_")
            .replace('/', "_")
            .replace('\\', "_");
        self.dir.join("nodes").join(format!("{}.json", safe))
    }
}

impl Store for FileStore {
    fn save_node(&self, id: &str, data: &[u8]) -> io::Result<()> {
        atomic_write(&self.node_path(id), data)
    }

    fn load_node(&self, id: &str) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.node_path(id)) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete_node(&self, id: &str) -> io::Result<()> {
        match fs::remove_file(self.node_path(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list_nodes(&self) -> io::Result<Vec<String>> {
        let nodes_dir = self.dir.join("nodes");
        let entries = match fs::read_dir(&nodes_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
        };

        let mut ids = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".json") || entry.file_type()?.is_dir() {
                continue;
            }
            // Read the file and extract the "id" field.
            let data = fs::read(entry.path())?;
            #[derive(Deserialize)]
            struct Peek {
                #[serde(default)]
                id: String,
            }
            if let Ok(peek) = serde_json::from_slice::<Peek>(&data) {
                if !peek.id.is_empty() {
                    ids.push(peek.id);
                }
            }
        }
        Ok(ids)
    }

    fn save_users(&self, users: &HashMap<String, UserRecord>) -> io::Result<()> {
        let data = serde_json::to_vec_pretty(users)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        atomic_write(&self.dir.join("users.json"), &data)
    }

    fn load_users(&self) -> io::Result<HashMap<String, UserRecord>> {
        match fs::read(self.dir.join("users.json")) {
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(e),
        }
    }

    fn save_server_key(&self, seed: &[u8]) -> io::Result<()> {
        let hex = seed
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        atomic_write(&self.dir.join("server_key"), format!("{}\n", hex).as_bytes())
    }

    fn load_server_key(&self) -> io::Result<Option<Vec<u8>>> {
        match fs::read_to_string(self.dir.join("server_key")) {
            Ok(s) => {
                let hex = s.trim();
                let bytes = (0..hex.len())
                    .step_by(2)
                    .map(|i| {
                        u8::from_str_radix(&hex[i..i + 2], 16)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
                    })
                    .collect::<io::Result<Vec<u8>>>()?;
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convenience: serialize a Node and save it.
pub fn save_node_json<S: Store, T: Serialize>(
    store: &S,
    id: &str,
    node: &T,
) -> io::Result<()> {
    let data = serde_json::to_vec_pretty(node)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    store.save_node(id, &data)
}

/// Convenience: load and deserialize a Node.
pub fn load_node_json<S: Store, T: for<'de> Deserialize<'de>>(
    store: &S,
    id: &str,
) -> io::Result<Option<T>> {
    match store.load_node(id)? {
        Some(data) => {
            let node = serde_json::from_slice(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok(Some(node))
        }
        None => Ok(None),
    }
}

/// Write data atomically: write to .tmp, fsync, rename.
fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Sync parent directory for durability.
    if let Some(parent) = path.parent() {
        if let Ok(d) = fs::File::open(parent) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Hex serialization for salt/hash fields
// ---------------------------------------------------------------------------

mod hex_bytes {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        serializer.serialize_str(&hex)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;

    #[test]
    fn file_store_save_load_node() {
        let dir = std::env::temp_dir().join("mimesis_test_store_1");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        let node = Node::new("alice@a.com".into());
        save_node_json(&store, "alice@a.com", &node).unwrap();

        let loaded: Node = load_node_json(&store, "alice@a.com").unwrap().unwrap();
        assert_eq!(loaded.id, "alice@a.com");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_list_nodes() {
        let dir = std::env::temp_dir().join("mimesis_test_store_2");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        save_node_json(&store, "alice@a.com", &Node::new("alice@a.com".into())).unwrap();
        save_node_json(&store, "bob@b.com", &Node::new("bob@b.com".into())).unwrap();

        let mut ids = store.list_nodes().unwrap();
        ids.sort();
        assert_eq!(ids, vec!["alice@a.com", "bob@b.com"]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_delete_node() {
        let dir = std::env::temp_dir().join("mimesis_test_store_3");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        save_node_json(&store, "alice@a.com", &Node::new("alice@a.com".into())).unwrap();
        assert!(store.load_node("alice@a.com").unwrap().is_some());

        store.delete_node("alice@a.com").unwrap();
        assert!(store.load_node("alice@a.com").unwrap().is_none());

        // Deleting non-existent is fine.
        store.delete_node("alice@a.com").unwrap();

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_load_missing_returns_none() {
        let dir = std::env::temp_dir().join("mimesis_test_store_4");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        assert!(store.load_node("nobody@x.com").unwrap().is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_users_roundtrip() {
        let dir = std::env::temp_dir().join("mimesis_test_store_5");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        let mut users = HashMap::new();
        users.insert(
            "alice".to_string(),
            UserRecord {
                name: "alice".into(),
                salt: vec![1, 2, 3, 4],
                hash: vec![0xde, 0xad, 0xbe, 0xef],
                admin: true,
            },
        );

        store.save_users(&users).unwrap();
        let loaded = store.load_users().unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded["alice"].admin);
        assert_eq!(loaded["alice"].salt, vec![1, 2, 3, 4]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_server_key_roundtrip() {
        let dir = std::env::temp_dir().join("mimesis_test_store_6");
        let _ = fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        let seed = vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
        store.save_server_key(&seed).unwrap();

        let loaded = store.load_server_key().unwrap().unwrap();
        assert_eq!(loaded, seed);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_node_path_sanitizes() {
        let dir = std::env::temp_dir().join("mimesis_test_store_7");
        let store = FileStore { dir: dir.clone() };
        let path = store.node_path("alice@example.com");
        assert!(!path.to_string_lossy().contains('@'));
        assert!(path.to_string_lossy().contains("_at_"));
    }
}
