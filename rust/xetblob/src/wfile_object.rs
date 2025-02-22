use async_trait::async_trait;
use core::ops::{Deref, DerefMut};
use gitxetcore::data_processing::*;
use merkledb::AsyncIterator;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
pub struct AsyncMpscIterator {
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}

#[async_trait]
impl AsyncIterator for AsyncMpscIterator {
    async fn next(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.receiver.recv().await)
    }
}
pub struct ActiveWriter {
    taskhandle: tokio::task::JoinHandle<Result<Vec<u8>, gitxetcore::errors::GitXetRepoError>>,
    sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

/// The writer can either be open or closed
/// If the writer is open it has an internal task to
/// handle smudging. Once its closed, it becomes a simple
/// Vec<u8> which holds the pointer file (or a passthrough)
enum WriterState {
    OpenState(ActiveWriter),
    ClosedState(Vec<u8>),
}
/// Describes a single Writable Xet file
/// This is thread safe and mutexed to allow for concurrent access
pub struct XetWFileObject {
    state: Mutex<WriterState>,
}

impl XetWFileObject {
    pub fn new_closed(content: Vec<u8>) -> Self {
        XetWFileObject {
            state: Mutex::new(WriterState::ClosedState(content)),
        }
    }
    pub fn new(filename: &str, translator: Arc<PointerFileTranslator>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let iterator = AsyncMpscIterator { receiver };
        let filename = std::path::PathBuf::from(&filename);
        let taskhandle =
            tokio::spawn(async move { translator.clean_file(&filename, iterator).await });
        XetWFileObject {
            state: Mutex::new(WriterState::OpenState(ActiveWriter {
                taskhandle,
                sender: Some(sender),
            })),
        }
    }
    /// returns true if files is closed
    pub async fn is_closed(&self) -> bool {
        matches!(self.state.lock().await.deref(), WriterState::ClosedState(_))
    }
    /// returns the final pointer file if files is closed
    pub async fn closed_state(&self) -> Option<Vec<u8>> {
        if let WriterState::ClosedState(ref v) = self.state.lock().await.deref() {
            Some(v.clone())
        } else {
            None
        }
    }
    /// Writes a collection of bytes
    pub async fn write(&self, buf: &[u8]) -> anyhow::Result<()> {
        if let WriterState::OpenState(ref mut state) = self.state.lock().await.deref_mut() {
            if let Some(ref sender) = state.sender {
                Ok(sender.send(buf.to_vec())?)
            } else {
                Err(anyhow::anyhow!("Writer already closed"))
            }
        } else {
            Err(anyhow::anyhow!("Writer already closed"))
        }
    }
    /// Closes the file
    pub async fn close(&self) -> anyhow::Result<()> {
        let mut self_state = self.state.lock().await;
        if matches!(self_state.deref(), WriterState::OpenState(_)) {
            // self state is current open. we need to close it.
            // To do so, we first swap it out so we can mutate
            // it to our desire
            let mut cur_state = WriterState::ClosedState(Vec::new());
            std::mem::swap(self_state.deref_mut(), &mut cur_state);
            let res = {
                if let WriterState::OpenState(mut writer) = cur_state {
                    // release the sender
                    writer.sender = None;
                    // wait on the task to close
                    writer.taskhandle.await??
                } else {
                    // this should not be possible.
                    // we matched against OpenState earlier
                    return Err(anyhow::anyhow!("Unexpected writer state"));
                }
            };
            // now we actually close.
            *self_state = WriterState::ClosedState(res);
            Ok(())
        } else {
            // if already closed, its a no-op
            Ok(())
        }
    }
}
