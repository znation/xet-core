#![cfg_attr(feature = "strict", deny(warnings))]

use std::{collections::BTreeMap, fs};

use toml::Value;
use tracing::warn;

const HEADER_PREFIX: &str = "# xet version ";
const CURRENT_VERSION: &str = "0";

/// A struct that wraps a Xet pointer file.
/// Xet pointer file format is a TOML file,
/// and the first line must be of the form "# xet version <x.y>"
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointerFile {
    /// The version string of the pointer file
    version_string: String,

    /// The initial path supplied (to a pointer file on disk)
    path: String,

    /// Whether the contents represent a valid pointer file.
    /// is_valid is true if and only if all of the following are true:
    /// * the first line starts with HEADER_PREFIX and then a version string
    /// * the whole contents are valid TOML
    /// * the TOML contains a top level key "hash" that is a String
    /// * the TOML contains a top level key "filesize" that is an Integer
    is_valid: bool,

    /// The Merkle hash of the file pointed to by this pointer file
    hash: String,

    /// The size of the file pointed to by this pointer file
    filesize: u64,
}

impl PointerFile {
    pub fn init_from_string(contents: &str, path: &str) -> PointerFile {
        let empty_string = "".to_string();

        // Start out valid by default.
        let mut is_valid = true;

        // Required members: hash and filesize.
        // Without these, not considered valid.
        let mut hash = empty_string.clone();
        let mut filesize: u64 = 0;

        let lines = contents.lines();
        let first_line: String = lines.take(1).collect();
        if !first_line.starts_with(HEADER_PREFIX) {
            // not a valid pointer file - doesn't start with header:
            // # xet version <x.y>
            is_valid = false;
            return PointerFile {
                version_string: empty_string,
                path: path.to_string(),
                is_valid,
                hash,
                filesize,
            };
        }

        let version_string = first_line[HEADER_PREFIX.len()..].to_string();
        if version_string != CURRENT_VERSION {
            warn!("Pointer file version {} encountered. Only version {} is supported. Please upgrade git-xet.", version_string, CURRENT_VERSION);
            // not a valid pointer file, doesn't start with header + version string
            is_valid = false;
            return PointerFile {
                version_string,
                path: path.to_string(),
                is_valid,
                hash,
                filesize,
            };
        }

        // Validated the header -- parse as TOML.
        let parsed = match contents.parse::<Value>() {
            Ok(v) => v,
            Err(_) => {
                is_valid = false;
                Value::String(empty_string)
            }
        };

        match parsed.get("hash") {
            Some(Value::String(s)) => {
                hash = s.to_string();
            }
            _ => {
                // did not find hash, or
                // found a non-string type for hash (unexpected)
                is_valid = false;
            }
        }

        match parsed.get("filesize") {
            Some(Value::Integer(i)) => {
                if *i < 0 {
                    // negative int should not be possible for filesize
                    is_valid = false;
                }
                filesize = *i as u64;
            }
            _ => {
                // did not find filesize, or
                // found a non-int type for filesize (unexpected)
                is_valid = false;
            }
        }

        PointerFile {
            version_string,
            path: path.to_string(),
            is_valid,
            hash,
            filesize,
        }
    }

    pub fn init_from_path(path: &str) -> PointerFile {
        let empty_string = "".to_string();
        let contents = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => {
                return PointerFile {
                    version_string: empty_string.clone(),
                    path: path.to_string(),
                    is_valid: false,
                    hash: empty_string,
                    filesize: 0,
                }
            }
        };

        PointerFile::init_from_string(&contents, path)
    }

    pub fn init_from_info(path: &str, hash: &str, filesize: u64) -> Self {
        Self {
            version_string: CURRENT_VERSION.to_string(),
            path: path.to_string(),
            is_valid: true,
            hash: hash.to_string(),
            filesize,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    pub fn hash(&self) -> &String {
        &self.hash
    }

    pub fn filesize(&self) -> u64 {
        self.filesize
    }
}

impl ToString for PointerFile {
    fn to_string(&self) -> String {
        if !self.is_valid {
            warn!("called to_string on invalid PointerFile");
            return "# invalid pointer file".to_string();
        }
        let mut contents = BTreeMap::<String, Value>::new();
        contents.insert("hash".to_string(), Value::String(self.hash.clone()));
        assert!(self.filesize <= i64::MAX as u64);
        contents.insert("filesize".to_string(), Value::Integer(self.filesize as i64));
        let contents_str = match toml::ser::to_string_pretty(&contents) {
            Ok(s) => s,
            Err(e) => panic!("expected to be able to serialize PointerFile, instead got error {e}"),
        };

        assert!(!self.version_string.is_empty());
        format!("{}{}\n{}", HEADER_PREFIX, self.version_string, contents_str)
    }
}

#[cfg(test)]
mod tests {
    const POINTER_FILE_VERSION: &str = "0";
    use crate::{PointerFile, HEADER_PREFIX};

    #[test]
    fn is_valid_pointer_file() {
        let empty_string = "".to_string();
        let mut test_contents = "# not a xet file\n42 is a number".to_string();
        let mut test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // not valid because it is missing the header prefix

        test_contents = format!("{}{}\n42 is a number", HEADER_PREFIX, POINTER_FILE_VERSION);
        test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // not valid because it doesn't contain valid TOML

        test_contents = format!("{}{}\nfoo = 'bar'", HEADER_PREFIX, POINTER_FILE_VERSION);
        test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // not valid because it doesn't contain hash or filesize

        test_contents = format!(
            "{}{}\nhash = '12345'\nfilesize = 678",
            HEADER_PREFIX, POINTER_FILE_VERSION
        );
        test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(test.is_valid()); // valid
    }

    #[test]
    fn empty_file() {
        let empty_string = "".to_string();
        let test = PointerFile::init_from_string(&empty_string, &empty_string);
        assert!(!test.is_valid()); // not valid because empty file
    }

    #[test]
    fn parses_correctly() {
        let empty_string = "".to_string();
        let test_contents = format!(
            "{}{}\nhash = '12345'\nfilesize = 678",
            HEADER_PREFIX, POINTER_FILE_VERSION
        );
        let test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(test.is_valid()); // valid
        assert_eq!(test.filesize(), 678);
        assert_eq!(test.hash(), "12345");
        assert_eq!(test.version_string, POINTER_FILE_VERSION);
    }

    #[test]
    fn is_serializable_and_deserializable() {
        let empty_string = "".to_string();
        let test_contents = format!(
            "{}{}\nhash = '12345'\nfilesize = 678",
            HEADER_PREFIX, POINTER_FILE_VERSION
        );
        let test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(test.is_valid()); // valid

        // make sure we can serialize it back out to string
        let serialized = test.to_string();

        // then read it back in, and make sure it's equal to the original
        let deserialized = PointerFile::init_from_string(&serialized, &empty_string);
        assert_eq!(test, deserialized);
    }

    #[test]
    fn test_new_version() {
        let empty_string = "".to_string();
        let test_contents = format!("{}{}\nhash = '12345'\nfilesize = 678", HEADER_PREFIX, "1.0");
        let test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // new version is not valid
    }
}
