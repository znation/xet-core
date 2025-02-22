use crate::config::XetConfig;
use crate::constants as gitxet_constants;
use crate::constants::POINTER_FILE_LIMIT;
use crate::data_processing::PointerFileTranslator;
use crate::xetmnt::xetfs_bare::{MOUNT_PASSTHROUGH_BYTES_READ, MOUNT_POINTER_BYTES_READ};
use async_trait::async_trait;
use intaglio::osstr::SymbolTable;
use intaglio::Symbol;
use nfsserve::fs_util::*;
use nfsserve::nfs::*;
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use pointer_file::PointerFile;
use std::collections::{BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::fs::Metadata;
use std::io::{BufWriter, SeekFrom};
use std::ops::Bound;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, error, info};
#[derive(Debug, Clone)]
struct FSEntry {
    name: Vec<Symbol>,
    fsmeta: fattr3,
    /// metadata when building the children list
    children_meta: fattr3,
    children: Option<BTreeSet<fileid3>>,
}

#[derive(Debug)]
struct FSMap {
    root: PathBuf,
    next_fileid: AtomicU64,
    intern: SymbolTable,
    id_to_path: HashMap<fileid3, FSEntry>,
    path_to_id: HashMap<Vec<Symbol>, fileid3>,
}

const PREFETCH_LOOKAHEAD: usize = gitxet_constants::PREFETCH_WINDOW_SIZE_BYTES as usize;

enum RefreshResult {
    /// The fileid was deleted
    Delete,
    /// The fileid needs to be reloaded. mtime has been updated, caches
    /// need to be evicted.
    Reload,
    /// Nothing has changed
    Noop,
}

fn to_xet_pointer_maybe(path: &Path) -> Result<Option<PointerFile>, nfsstat3> {
    let len = path
        .symlink_metadata()
        .or(Err(nfsstat3::NFS3ERR_NOENT))?
        .len();

    let pointer = if len < POINTER_FILE_LIMIT as u64 {
        std::fs::read(path)
            .ok()
            .as_ref()
            .and_then(|contents| std::str::from_utf8(contents).ok())
            .and_then(|utf8str| {
                let ptr = PointerFile::init_from_string(utf8str, "");
                if ptr.is_valid() {
                    Some(ptr)
                } else {
                    None
                }
            })
    } else {
        None
    };
    Ok(pointer)
}

/// transforms a file Metadata into a Xet-compatible fattr3
/// translating pointer files
fn to_xet_metadata(fid: fileid3, path: &Path, metadata: &Metadata) -> fattr3 {
    let mut meta = metadata_to_fattr3(fid, metadata);
    if meta.size <= POINTER_FILE_LIMIT as u64 {
        if let Some(pointer) = std::fs::read(path)
            .ok()
            .as_ref()
            .and_then(|contents| std::str::from_utf8(contents).ok())
            .map(|utf8str| PointerFile::init_from_string(utf8str, ""))
        {
            if pointer.is_valid() {
                meta.size = pointer.filesize();
            }
        }
    }
    meta
}

async fn checkout_xetfile(
    pfilereader: &PointerFileTranslator,
    fullpath: &Path,
    pointer: &PointerFile,
) -> Result<(), anyhow::Error> {
    // we need to checkout
    let mut tempfile = tempfile::Builder::new()
        .prefix(fullpath.file_name().unwrap())
        .suffix(".tmp")
        .tempfile_in(fullpath.parent().unwrap())?;
    {
        let mut f = BufWriter::new(&mut tempfile);

        pfilereader
            .smudge_file_from_pointer(&fullpath.to_path_buf(), pointer, &mut f, None)
            .await?;
    }

    tempfile.persist(fullpath)?;

    Ok(())
}

impl FSMap {
    fn new(root: PathBuf) -> FSMap {
        // create root entry
        let root_entry = FSEntry {
            name: Vec::new(),
            fsmeta: metadata_to_fattr3(1, &root.metadata().unwrap()),
            children_meta: metadata_to_fattr3(1, &root.metadata().unwrap()),
            children: None,
        };
        FSMap {
            root,
            next_fileid: AtomicU64::new(1),
            intern: SymbolTable::new(),
            id_to_path: HashMap::from([(0, root_entry)]),
            path_to_id: HashMap::from([(Vec::new(), 0)]),
        }
    }
    async fn sym_to_path(&self, symlist: &[Symbol]) -> PathBuf {
        let mut ret = self.root.clone();
        for i in symlist.iter() {
            ret.push(self.intern.get(*i).unwrap());
        }
        ret
    }

    async fn sym_to_fname(&self, symlist: &[Symbol]) -> OsString {
        if let Some(x) = symlist.last() {
            self.intern.get(*x).unwrap().into()
        } else {
            "".into()
        }
    }

    fn collect_all_children(&self, id: fileid3, ret: &mut Vec<fileid3>) {
        ret.push(id);
        if let Some(entry) = self.id_to_path.get(&id) {
            if let Some(ref ch) = entry.children {
                for i in ch.iter() {
                    self.collect_all_children(*i, ret);
                }
            }
        }
    }

    fn delete_entry(&mut self, id: fileid3) {
        let mut children = Vec::new();
        self.collect_all_children(id, &mut children);
        for i in children.iter() {
            if let Some(ent) = self.id_to_path.remove(i) {
                self.path_to_id.remove(&ent.name);
            }
        }
    }

    fn find_entry(&self, id: fileid3) -> Result<FSEntry, nfsstat3> {
        Ok(self
            .id_to_path
            .get(&id)
            .ok_or(nfsstat3::NFS3ERR_NOENT)?
            .clone())
    }
    fn find_entry_mut(&mut self, id: fileid3) -> Result<&mut FSEntry, nfsstat3> {
        self.id_to_path.get_mut(&id).ok_or(nfsstat3::NFS3ERR_NOENT)
    }
    async fn find_child(&self, id: fileid3, filename: &[u8]) -> Result<fileid3, nfsstat3> {
        let mut name = self
            .id_to_path
            .get(&id)
            .ok_or(nfsstat3::NFS3ERR_NOENT)?
            .name
            .clone();
        name.push(
            self.intern
                .check_interned(OsStr::from_bytes(filename))
                .ok_or(nfsstat3::NFS3ERR_NOENT)?,
        );
        Ok(*self.path_to_id.get(&name).ok_or(nfsstat3::NFS3ERR_NOENT)?)
    }
    async fn refresh_entry(&mut self, id: fileid3) -> Result<RefreshResult, nfsstat3> {
        let entry = self
            .id_to_path
            .get(&id)
            .ok_or(nfsstat3::NFS3ERR_NOENT)?
            .clone();
        let path = self.sym_to_path(&entry.name).await;
        //
        if !exists_no_traverse(&path) {
            self.delete_entry(id);
            debug!("Deleting entry A {:?}: {:?}. Ent: {:?}", id, path, entry);
            return Ok(RefreshResult::Delete);
        }

        let meta = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let meta = to_xet_metadata(id, &path, &meta);
        if !fattr3_differ(&meta, &entry.fsmeta) {
            return Ok(RefreshResult::Noop);
        }
        // If we get here we have modifications
        if entry.fsmeta.ftype as u32 != meta.ftype as u32 {
            // if the file type changed ex: file->dir or dir->file
            // really the entire file has been replaced.
            // we expire the entire id
            debug!(
                "File Type Mismatch FT {:?} : {:?} vs {:?}",
                id, entry.fsmeta.ftype, meta.ftype
            );
            debug!(
                "File Type Mismatch META {:?} : {:?} vs {:?}",
                id, entry.fsmeta, meta
            );
            self.delete_entry(id);
            debug!("Deleting entry B {:?}: {:?}. Ent: {:?}", id, path, entry);
            return Ok(RefreshResult::Delete);
        }
        // inplace modification.
        // update metadata
        self.id_to_path.get_mut(&id).unwrap().fsmeta = meta;
        debug!("Reloading entry {:?}: {:?}. Ent: {:?}", id, path, entry);
        Ok(RefreshResult::Reload)
    }
    async fn refresh_dir_list(&mut self, id: fileid3) -> Result<(), nfsstat3> {
        let entry = self
            .id_to_path
            .get(&id)
            .ok_or(nfsstat3::NFS3ERR_NOENT)?
            .clone();
        // if there are children and the metadata did not change
        if entry.children.is_some() && !fattr3_differ(&entry.children_meta, &entry.fsmeta) {
            return Ok(());
        }
        if !matches!(entry.fsmeta.ftype, ftype3::NF3DIR) {
            return Ok(());
        }
        let mut cur_path = entry.name.clone();
        let path = self.sym_to_path(&entry.name).await;
        let mut new_children: Vec<u64> = Vec::new();
        debug!("Relisting entry {:?}: {:?}. Ent: {:?}", id, path, entry);
        if let Ok(mut listing) = tokio::fs::read_dir(&path).await {
            while let Some(entry) = listing
                .next_entry()
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
            {
                let sym = self.intern.intern(entry.file_name()).unwrap();
                cur_path.push(sym);
                let mut cur_path_expanded = path.clone();
                cur_path_expanded.push(entry.file_name());
                let meta = entry.metadata().await.unwrap();
                let next_id = self.create_entry(&cur_path, &cur_path_expanded, meta).await;
                new_children.push(next_id);
                cur_path.pop();
            }
            self.id_to_path
                .get_mut(&id)
                .ok_or(nfsstat3::NFS3ERR_NOENT)?
                .children = Some(BTreeSet::from_iter(new_children.into_iter()));
        }

        Ok(())
    }

    async fn create_entry(
        &mut self,
        fullpath: &Vec<Symbol>,
        expandedpath: &Path,
        meta: Metadata,
    ) -> fileid3 {
        let next_id = if let Some(chid) = self.path_to_id.get(fullpath) {
            if let Some(chent) = self.id_to_path.get_mut(chid) {
                chent.fsmeta = to_xet_metadata(*chid, expandedpath, &meta);
            }
            *chid
        } else {
            // path does not exist
            let next_id = self.next_fileid.fetch_add(1, Ordering::Relaxed);
            let metafattr = to_xet_metadata(next_id, expandedpath, &meta);
            let new_entry = FSEntry {
                name: fullpath.clone(),
                fsmeta: metafattr,
                children_meta: metafattr,
                children: None,
            };
            debug!("creating new entry {:?}: {:?}", next_id, meta);
            self.id_to_path.insert(next_id, new_entry);
            self.path_to_id.insert(fullpath.clone(), next_id);
            next_id
        };
        next_id
    }
}
pub struct XetFSWritable {
    fsmap: tokio::sync::Mutex<FSMap>,
    pfilereader: PointerFileTranslator,
    /// we only checkout 1 file at a time to avoid
    /// parallel writes from issuing multiple checkouts of
    /// the same file
    checkout_lock: tokio::sync::Mutex<()>,
    prefetch: usize,
}

/// Enumeration for the create_fs_object method
enum CreateFSObject {
    /// Creates a directory
    Directory,
    /// Creates a file with a set of attributes
    File(sattr3),
    /// Creates an exclusive file with a set of attributes
    Exclusive,
    /// Creates a symlink with a set of attributes to a target location
    Symlink((sattr3, nfspath3)),
}
impl XetFSWritable {
    pub async fn new(
        root: &Path,
        cfg: &XetConfig,
        prefetch: usize,
    ) -> Result<XetFSWritable, anyhow::Error> {
        let pfile = PointerFileTranslator::from_config(cfg).await?;
        Ok(XetFSWritable {
            fsmap: tokio::sync::Mutex::new(FSMap::new(root.to_path_buf())),
            pfilereader: pfile,
            checkout_lock: tokio::sync::Mutex::new(()),
            prefetch,
        })
    }

    /// creates a FS object in a given directory and of a given type
    /// Updates as much metadata as we can in-place
    async fn create_fs_object(
        &self,
        dirid: fileid3,
        objectname: &filename3,
        object: &CreateFSObject,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let mut fsmap = self.fsmap.lock().await;
        let ent = fsmap.find_entry(dirid)?;
        let mut path = fsmap.sym_to_path(&ent.name).await;
        let objectname_osstr = OsStr::from_bytes(objectname).to_os_string();
        path.push(&objectname_osstr);

        match object {
            CreateFSObject::Directory => {
                debug!("mkdir {:?}", path);
                if exists_no_traverse(&path) {
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }
                tokio::fs::create_dir(&path)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            }
            CreateFSObject::File(setattr) => {
                debug!("create {:?}", path);
                let file = std::fs::File::create(&path).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let _ = file_setattr(&file, setattr).await;
            }
            CreateFSObject::Exclusive => {
                debug!("create exclusive {:?}", path);
                let _ = std::fs::File::options()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(|_| nfsstat3::NFS3ERR_EXIST)?;
            }
            CreateFSObject::Symlink((_, target)) => {
                debug!("symlink {:?} {:?}", path, target);
                if exists_no_traverse(&path) {
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }
                tokio::fs::symlink(OsStr::from_bytes(target), &path)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                // we do not set attributes on symlinks
            }
        }

        let _ = fsmap.refresh_entry(dirid).await;

        let sym = fsmap.intern.intern(objectname_osstr).unwrap();
        let mut name = ent.name.clone();
        name.push(sym);
        let meta = path.symlink_metadata().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let fileid = fsmap.create_entry(&name, &path, meta.clone()).await;

        // update the children list
        if let Some(ref mut children) = fsmap
            .id_to_path
            .get_mut(&dirid)
            .ok_or(nfsstat3::NFS3ERR_NOENT)?
            .children
        {
            children.insert(fileid);
        }
        Ok((fileid, metadata_to_fattr3(fileid, &meta)))
    }
}

#[async_trait]
impl NFSFileSystem for XetFSWritable {
    fn root_dir(&self) -> fileid3 {
        0
    }
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let mut fsmap = self.fsmap.lock().await;
        if let Ok(id) = fsmap.find_child(dirid, filename).await {
            if fsmap.id_to_path.contains_key(&id) {
                return Ok(id);
            }
        }
        // Optimize for negative lookups.
        // See if the file actually exists on the filesystem
        let dirent = fsmap.find_entry(dirid)?;
        let mut path = fsmap.sym_to_path(&dirent.name).await;
        let objectname_osstr = OsStr::from_bytes(filename).to_os_string();
        path.push(&objectname_osstr);
        if !exists_no_traverse(&path) {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        // ok the file actually exists.
        // that means something changed under me probably.
        // refresh.

        if let RefreshResult::Delete = fsmap.refresh_entry(dirid).await? {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        let _ = fsmap.refresh_dir_list(dirid).await;

        fsmap.find_child(dirid, filename).await
        //debug!("lookup({:?}, {:?})", dirid, filename);

        //debug!(" -- lookup result {:?}", res);
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        //debug!("Stat query {:?}", id);
        let mut fsmap = self.fsmap.lock().await;
        if let RefreshResult::Delete = fsmap.refresh_entry(id).await? {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        let ent = fsmap.find_entry(id)?;
        let path = fsmap.sym_to_path(&ent.name).await;
        debug!("Stat {:?}: {:?}", path, ent);
        Ok(ent.fsmeta)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let fsmap = self.fsmap.lock().await;
        let ent = fsmap.find_entry(id)?;
        let path = fsmap.sym_to_path(&ent.name).await;
        drop(fsmap);
        // check if its a pointer file
        let pointer = to_xet_pointer_maybe(&path)?;
        if let Some(pointer) = pointer {
            // resolve pointer
            let mut start = offset as usize;
            let mut end = offset as usize + count as usize;
            let len = pointer.filesize() as usize;
            let eof = end >= len;
            if start >= len {
                start = len;
            }
            if end > len {
                end = len;
            }
            MOUNT_POINTER_BYTES_READ.inc_by((end - start) as u64);

            let mut output: Vec<u8> = Vec::new();
            for ctr in 1..(self.prefetch + 1) {
                if start + ctr * PREFETCH_LOOKAHEAD >= len {
                    break;
                }
                if self
                    .pfilereader
                    .prefetch(&pointer, (start + ctr * PREFETCH_LOOKAHEAD) as u64)
                    .await
                    .unwrap()
                {
                    break;
                }
            }
            self.pfilereader
                .smudge_file_from_pointer(
                    &PathBuf::new(),
                    &pointer,
                    &mut output,
                    Some((start, end)),
                )
                .await
                .or(Err(nfsstat3::NFS3ERR_IO))?;
            Ok((output, eof))
        } else {
            let mut f = File::open(&path).await.or(Err(nfsstat3::NFS3ERR_NOENT))?;
            let len = f.metadata().await.or(Err(nfsstat3::NFS3ERR_NOENT))?.len();
            let mut start = offset;
            let mut end = offset + count as u64;
            let eof = end >= len;
            if start >= len {
                start = len;
            }
            if end > len {
                end = len;
            }
            MOUNT_PASSTHROUGH_BYTES_READ.inc_by(end - start);
            f.seek(SeekFrom::Start(start))
                .await
                .or(Err(nfsstat3::NFS3ERR_IO))?;
            let mut buf = vec![0; (end - start) as usize];
            f.read_exact(&mut buf).await.or(Err(nfsstat3::NFS3ERR_IO))?;
            Ok((buf, eof))
        }
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let mut fsmap = self.fsmap.lock().await;
        fsmap.refresh_entry(dirid).await?;
        fsmap.refresh_dir_list(dirid).await?;

        let entry = fsmap.find_entry(dirid)?;
        if !matches!(entry.fsmeta.ftype, ftype3::NF3DIR) {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }
        debug!("readdir({:?}, {:?})", entry, start_after);
        // we must have children here
        let children = entry.children.ok_or(nfsstat3::NFS3ERR_IO)?;

        let mut ret = ReadDirResult {
            entries: Vec::new(),
            end: false,
        };

        let range_start = if start_after > 0 {
            Bound::Excluded(start_after)
        } else {
            Bound::Unbounded
        };

        let remaining_length = children.range((range_start, Bound::Unbounded)).count();
        let path = fsmap.sym_to_path(&entry.name).await;
        debug!("path: {:?}", path);
        debug!("children len: {:?}", children.len());
        debug!("remaining_len : {:?}", remaining_length);
        for i in children.range((range_start, Bound::Unbounded)) {
            let fileid = *i;
            let fileent = fsmap.find_entry(fileid)?;
            let name = fsmap.sym_to_fname(&fileent.name).await;
            debug!("\t --- {:?} {:?}", fileid, name);
            ret.entries.push(DirEntry {
                fileid,
                name: name.as_bytes().into(),
                attr: fileent.fsmeta,
            });
            if ret.entries.len() >= max_entries {
                break;
            }
        }
        if ret.entries.len() == remaining_length {
            ret.end = true;
        }
        debug!("readdir_result:{:?}", ret);

        Ok(ret)
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let mut fsmap = self.fsmap.lock().await;
        let entry = fsmap.find_entry(id)?;
        let path = fsmap.sym_to_path(&entry.name).await;
        path_setattr(&path, &setattr).await?;

        // I have to lookup a second time to update
        let metadata = path.symlink_metadata().or(Err(nfsstat3::NFS3ERR_IO))?;
        let metadata = to_xet_metadata(id, &path, &metadata);
        if let Ok(entry) = fsmap.find_entry_mut(id) {
            entry.fsmeta = metadata;
        }
        Ok(metadata)
    }
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let fsmap = self.fsmap.lock().await;
        let ent = fsmap.find_entry(id)?;
        let path = fsmap.sym_to_path(&ent.name).await;
        drop(fsmap);
        {
            // we only checkout 1 file at a time to avoid
            // parallel writes from issuing multiple checkouts of
            // the same file. We do a double-checked locking here
            // against the filesystem so a write to file in the midst of being
            // checked out will not block every other write.
            // Much optimizations available here
            if to_xet_pointer_maybe(&path)?.is_some() {
                let _checkoutlock = self.checkout_lock.lock().await;
                if let Some(pointer) = to_xet_pointer_maybe(&path)? {
                    if let Err(e) = checkout_xetfile(&self.pfilereader, &path, &pointer).await {
                        error!("Unable to checkout xet file {:?}", e);
                        return Err(nfsstat3::NFS3ERR_IO);
                    }
                }
            }
        }
        // after this the file should be a normal file
        debug!("write to init {:?}", path);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(|e| {
                debug!("Unable to open {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?;
        f.seek(SeekFrom::Start(offset)).await.map_err(|e| {
            debug!("Unable to seek {:?}", e);
            nfsstat3::NFS3ERR_IO
        })?;
        f.write_all(data).await.map_err(|e| {
            debug!("Unable to write {:?}", e);
            nfsstat3::NFS3ERR_IO
        })?;
        debug!("write to {:?} {:?} {:?}", path, offset, data.len());
        let _ = f.flush().await;
        let _ = f.sync_all().await;
        let meta = f.metadata().await.or(Err(nfsstat3::NFS3ERR_IO))?;
        Ok(metadata_to_fattr3(id, &meta))
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        setattr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.create_fs_object(dirid, filename, &CreateFSObject::File(setattr))
            .await
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Ok(self
            .create_fs_object(dirid, filename, &CreateFSObject::Exclusive)
            .await?
            .0)
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let mut fsmap = self.fsmap.lock().await;
        let ent = fsmap.find_entry(dirid)?;
        let mut path = fsmap.sym_to_path(&ent.name).await;
        path.push(OsStr::from_bytes(filename));
        if let Ok(meta) = path.symlink_metadata() {
            if meta.is_dir() {
                tokio::fs::remove_dir(&path)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            } else {
                tokio::fs::remove_file(&path)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            }

            let filesym = fsmap
                .intern
                .intern(OsStr::from_bytes(filename).to_os_string())
                .unwrap();
            let mut sympath = ent.name.clone();
            sympath.push(filesym);
            if let Some(fileid) = fsmap.path_to_id.get(&sympath).copied() {
                // update the fileid -> path
                // and the path -> fileid mappings for the deleted file
                fsmap.id_to_path.remove(&fileid);
                fsmap.path_to_id.remove(&sympath);
                // we need to update the children listing for the directories
                if let Ok(dirent_mut) = fsmap.find_entry_mut(dirid) {
                    if let Some(ref mut fromch) = dirent_mut.children {
                        fromch.remove(&fileid);
                    }
                }
            }

            let _ = fsmap.refresh_entry(dirid).await;
        } else {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }

        Ok(())
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let mut fsmap = self.fsmap.lock().await;

        let from_dirent = fsmap.find_entry(from_dirid)?;
        let mut from_path = fsmap.sym_to_path(&from_dirent.name).await;
        from_path.push(OsStr::from_bytes(from_filename));

        let to_dirent = fsmap.find_entry(to_dirid)?;
        let mut to_path = fsmap.sym_to_path(&to_dirent.name).await;
        // to folder must exist
        if !exists_no_traverse(&to_path) {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        to_path.push(OsStr::from_bytes(to_filename));

        // src path must exist
        if !exists_no_traverse(&from_path) {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        debug!("Rename {:?} to {:?}", from_path, to_path);
        tokio::fs::rename(&from_path, &to_path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // TODO. I think we can do better here
        // Basically, if we see that the merkledb file has changed
        // we need to reload it.
        if to_filename.0 == b"merkledb.db" {
            info!("Reloading merkledb");
            self.pfilereader.reload_mdb().await;
        }

        let oldsym = fsmap
            .intern
            .intern(OsStr::from_bytes(from_filename).to_os_string())
            .unwrap();
        let newsym = fsmap
            .intern
            .intern(OsStr::from_bytes(to_filename).to_os_string())
            .unwrap();

        let mut from_sympath = from_dirent.name.clone();
        from_sympath.push(oldsym);
        let mut to_sympath = to_dirent.name.clone();
        to_sympath.push(newsym);
        if let Some(fileid) = fsmap.path_to_id.get(&from_sympath).copied() {
            // update the fileid -> path
            // and the path -> fileid mappings for the new file
            fsmap.id_to_path.get_mut(&fileid).unwrap().name = to_sympath.clone();
            fsmap.path_to_id.remove(&from_sympath);
            fsmap.path_to_id.insert(to_sympath, fileid);
            if to_dirid != from_dirid {
                // moving across directories.
                // we need to update the children listing for the directories
                if let Ok(from_dirent_mut) = fsmap.find_entry_mut(from_dirid) {
                    if let Some(ref mut fromch) = from_dirent_mut.children {
                        fromch.remove(&fileid);
                    }
                }
                if let Ok(to_dirent_mut) = fsmap.find_entry_mut(to_dirid) {
                    if let Some(ref mut toch) = to_dirent_mut.children {
                        toch.insert(fileid);
                    }
                }
            }
        }
        let _ = fsmap.refresh_entry(from_dirid).await;
        if to_dirid != from_dirid {
            let _ = fsmap.refresh_entry(to_dirid).await;
        }

        Ok(())
    }
    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.create_fs_object(dirid, dirname, &CreateFSObject::Directory)
            .await
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.create_fs_object(
            dirid,
            linkname,
            &CreateFSObject::Symlink((*attr, symlink.clone())),
        )
        .await
    }
    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let fsmap = self.fsmap.lock().await;
        let ent = fsmap.find_entry(id)?;
        let path = fsmap.sym_to_path(&ent.name).await;
        drop(fsmap);
        if path.is_symlink() {
            if let Ok(target) = path.read_link() {
                Ok(target.as_os_str().as_bytes().into())
            } else {
                Err(nfsstat3::NFS3ERR_IO)
            }
        } else {
            Err(nfsstat3::NFS3ERR_BADTYPE)
        }
    }
}
