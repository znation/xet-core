use tracing::{info, info_span};
use tracing_futures::Instrument;

use crate::config::XetConfig;
use crate::constants;
use crate::data_processing::PointerFileTranslator;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use crate::stream::git_stream::GitStreamInterface;

/// Filter command handler
pub async fn filter_command(config: XetConfig) -> errors::Result<()> {
    eprintln!("git-xet {} filter started", constants::CURRENT_VERSION);
    info!("Establishing Git Handshake.");

    // Sync up the notes to the local mdb
    // TODO: try to remove config cloning
    let mut repo = GitRepo::open(config.clone())?;

    let (changed, git_attr_changed) = repo.initialize(false, false).await?;

    if changed {
        info!("Git repo updated.");

        if git_attr_changed {
            eprintln!("Note: Xet has updated the .gitattributes file to properly track data files in your repo.");
            eprintln!("Please commit the new .gitattributes file to the repo using git add .gitattributes && git commit.");
        }
    }

    info!("MDB version {:?}", repo.mdb_version);
    repo.sync_notes_to_dbs().await?;

    let repo = PointerFileTranslator::from_config(&config).await?;
    let mut event_loop =
        GitStreamInterface::new_with_progress(std::io::stdin(), std::io::stdout(), repo);
    event_loop
        .establish_git_handshake()
        .instrument(info_span!("git_handshake"))
        .await?;
    info!("XET: Git Handshake established.");

    event_loop.run_git_event_loop().await?;
    event_loop
        .repo
        .write() // acquire write lock
        .await
        .finalize()
        .instrument(info_span!("finalize"))
        .await?;
    event_loop.repo.read().await.print_stats();
    Ok(())
}
