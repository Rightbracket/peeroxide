//! v2 receiver: BFS fetch over the index tree with mmap output (`--output`)
//! or streaming stdout output.
//!
//! Spec: see *Fetch Protocol (Receiver)* in `DEADDROP_V2.md (and `docs/src/dd/`)`.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use peeroxide::KeyPair;
use peeroxide_dht::hyperdht::HyperDhtHandle;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::cmd::deaddrop::progress::reporter::ProgressReporter;
use crate::cmd::deaddrop::progress::state::ProgressState;

use super::super::GetArgs;
use super::keys::{ack_topic, need_topic};
use super::need::{coalesce_missing_ranges, encode_need_list};
use super::stream::StreamSink;
use super::tree::{compute_layout, data_chunk_count};
use super::wire::{
    decode_data_chunk, decode_non_root_index, decode_root_index, NON_ROOT_INDEX_SLOT_CAP,
};
use super::PARALLEL_FETCH_CAP;

/// Per-task fetch result variants.
enum TaskOutcome {
    Index {
        remaining_depth: u32,
        base: u64,
        end: u64,
        result: Result<Vec<u8>, String>,
    },
    Data {
        position: u64,
        result: Result<Vec<u8>, String>,
    },
}

/// Per-data-position state. Drives need-list publishing: only `Failed`
/// positions are advertised, so the sender is not asked to re-publish
/// chunks the receiver hasn't actually attempted yet.
#[derive(Clone, Debug)]
enum ChunkState {
    /// No fetch has been scheduled — the parent index chunk hasn't been
    /// decoded yet, so we don't know the chunk's address.
    Unscheduled,
    /// A fetch task is running (or queued behind the parallel-fetch
    /// semaphore) for this address.
    InFlight { address: [u8; 32] },
    /// The fetch task returned an error. Eligible for re-spawn on the
    /// next need-list publish cycle.
    Failed { address: [u8; 32] },
    /// The chunk has been successfully decoded and written.
    Done,
}

/// Output destination strategy.
enum OutputSink {
    /// Memory-mapped output file (write-by-position). The mapping is rounded
    /// up to whole chunks; `file_size` is the length to truncate to.
    File {
        mmap: memmap2::MmapMut,
        file_size: u64,
        temp_path: std::path::PathBuf,
        final_path: std::path::PathBuf,
    },
    /// Empty output file: no mmap, just create-and-rename at finalize.
    EmptyFile {
        temp_path: std::path::PathBuf,
        final_path: std::path::PathBuf,
    },
    /// Streaming stdout via reorder buffer.
    Stdout(StreamSink),
    /// Empty stdout: write nothing.
    EmptyStdout,
}

impl OutputSink {
    /// Accept a data chunk's payload at its file-order position.
    /// Returns Err if I/O fails.
    fn accept(&mut self, position: u64, payload: &[u8]) -> Result<(), String> {
        match self {
            OutputSink::File { mmap, .. } => {
                use super::wire::DATA_PAYLOAD_MAX;
                let offset = (position * DATA_PAYLOAD_MAX as u64) as usize;
                if offset + payload.len() > mmap.len() {
                    return Err(format!(
                        "chunk at position {position} extends past mmap end"
                    ));
                }
                mmap[offset..offset + payload.len()].copy_from_slice(payload);
                Ok(())
            }
            OutputSink::Stdout(sink) => {
                let to_emit = sink.accept(position, payload.to_vec());
                use std::io::Write;
                let mut out = std::io::stdout().lock();
                for bytes in to_emit {
                    out.write_all(&bytes)
                        .map_err(|e| format!("stdout write failed: {e}"))?;
                }
                out.flush().map_err(|e| format!("stdout flush failed: {e}"))?;
                Ok(())
            }
            OutputSink::EmptyFile { .. } | OutputSink::EmptyStdout => {
                // Nothing to write — empty-file callers shouldn't pass any chunks
                // (N=0 means no data layer). Be permissive: just no-op.
                Ok(())
            }
        }
    }

    /// Finalize the output (flush mmap, truncate, atomic rename; or no-op for
    /// stdout). If it fails after the temp file was written, the temp file is
    /// removed.
    fn finalize(self) -> Result<(), String> {
        match self {
            OutputSink::File {
                mmap,
                file_size,
                temp_path,
                final_path,
            } => {
                let result = (|| {
                    mmap.flush().map_err(|e| format!("mmap flush failed: {e}"))?;
                    // Unmap before truncating: Windows refuses to shrink a
                    // file that still has a mapped view.
                    drop(mmap);
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&temp_path)
                        .and_then(|file| file.set_len(file_size))
                        .map_err(|e| format!("failed to truncate temp file: {e}"))?;
                    std::fs::rename(&temp_path, &final_path)
                        .map_err(|e| format!("rename to {final_path:?} failed: {e}"))
                })();
                if result.is_err() {
                    let _ = std::fs::remove_file(&temp_path);
                }
                result
            }
            OutputSink::EmptyFile {
                temp_path,
                final_path,
            } => {
                // Create an empty file at temp_path, then rename. Remove it
                // only if we wrote it, so a failed write deletes nothing.
                std::fs::write(&temp_path, [])
                    .map_err(|e| format!("failed to write empty temp file: {e}"))?;
                std::fs::rename(&temp_path, &final_path).map_err(|e| {
                    let _ = std::fs::remove_file(&temp_path);
                    format!("rename to {final_path:?} failed: {e}")
                })
            }
            OutputSink::Stdout(sink) => {
                use std::io::Write;
                let _ = sink; // ensure consumed
                std::io::stdout()
                    .flush()
                    .map_err(|e| format!("stdout flush failed: {e}"))?;
                Ok(())
            }
            OutputSink::EmptyStdout => Ok(()),
        }
    }

    /// Discard the output without committing (used on error before finalize).
    fn discard(self) {
        match self {
            OutputSink::File {
                mmap, temp_path, ..
            } => {
                drop(mmap);
                let _ = std::fs::remove_file(&temp_path);
            }
            OutputSink::EmptyFile { temp_path, .. } => {
                let _ = std::fs::remove_file(&temp_path);
            }
            OutputSink::Stdout(_) | OutputSink::EmptyStdout => {}
        }
    }
}

/// Build the appropriate `OutputSink` for the user's request.
fn open_output_sink(args: &GetArgs, file_size: u64) -> Result<OutputSink, String> {
    use super::wire::DATA_PAYLOAD_MAX;
    if let Some(path) = args.output.as_ref() {
        let path = std::path::PathBuf::from(path);
        let dir = path.parent().unwrap_or_else(|| std::path::Path::new(".")).to_path_buf();
        let temp_name = format!(".peeroxide-pickup-{}", std::process::id());
        let temp_path = dir.join(temp_name);

        if file_size == 0 {
            return Ok(OutputSink::EmptyFile {
                temp_path,
                final_path: path,
            });
        }

        // Allocate output file. We size it to N * DATA_PAYLOAD_MAX so that
        // each chunk writes to its position * 998 byte offset; the last
        // chunk may overshoot file_size by up to 998 bytes. `finalize`
        // truncates to file_size before rename.
        let n = data_chunk_count(file_size);
        let alloc_size = (n.saturating_mul(DATA_PAYLOAD_MAX as u64)).max(file_size);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| format!("failed to open temp file {temp_path:?}: {e}"))?;
        file.set_len(alloc_size)
            .map_err(|e| format!("failed to size temp file: {e}"))?;
        let mmap = unsafe {
            memmap2::MmapMut::map_mut(&file).map_err(|e| format!("mmap failed: {e}"))?
        };
        drop(file); // mmap holds the underlying mapping
        Ok(OutputSink::File {
            mmap,
            file_size,
            temp_path,
            final_path: path,
        })
    } else if file_size == 0 {
        Ok(OutputSink::EmptyStdout)
    } else {
        let n = data_chunk_count(file_size);
        Ok(OutputSink::Stdout(StreamSink::new(n)))
    }
}

/// Fetch a single mutable record with exponential backoff, bounded by `deadline`.
async fn fetch_mutable_with_retry(
    handle: &HyperDhtHandle,
    pk: &[u8; 32],
    deadline: tokio::time::Instant,
) -> Result<Vec<u8>, String> {
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(15);
    loop {
        match handle.mutable_get(pk, 0).await {
            Ok(Some(r)) => return Ok(r.value),
            Ok(None) => {}
            Err(e) => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(format!("mutable_get failed: {e}"));
                }
            }
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timeout".to_string());
        }
        let sleep = backoff.min(deadline.saturating_duration_since(now));
        tokio::time::sleep(sleep).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Fetch a single immutable record (data chunk) with exponential backoff.
async fn fetch_immutable_with_retry(
    handle: &HyperDhtHandle,
    address: &[u8; 32],
    deadline: tokio::time::Instant,
) -> Result<Vec<u8>, String> {
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(15);
    loop {
        match handle.immutable_get(*address).await {
            Ok(Some(bytes)) => return Ok(bytes),
            Ok(None) => {}
            Err(e) => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(format!("immutable_get failed: {e}"));
                }
            }
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timeout".to_string());
        }
        let sleep = backoff.min(deadline.saturating_duration_since(now));
        tokio::time::sleep(sleep).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Receiver-side need-list keepalive: announces the receiver's ephemeral
/// keypair on the need topic on a refresh cycle while the get is in
/// progress.
async fn run_need_announcer(
    handle: HyperDhtHandle,
    need_topic_key: [u8; 32],
    need_kp: KeyPair,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let interval = Duration::from_secs(60);
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            _ = async {
                if let Err(e) = handle.announce(need_topic_key, &need_kp, &[]).await {
                    eprintln!("  warning: need-topic announce failed: {e}");
                }
                tokio::time::sleep(interval).await;
            } => {}
        }
    }
}

/// Top-level GET entry point. Already given the fetched root chunk bytes
/// from `mod.rs::run_get` (which had to read the version byte to dispatch).
#[allow(clippy::too_many_arguments)]
pub async fn get_from_root(
    root_data: Vec<u8>,
    root_pk: [u8; 32],
    handle: HyperDhtHandle,
    task_handle: tokio::task::JoinHandle<
        Result<(), peeroxide_dht::hyperdht::HyperDhtError>,
    >,
    args: &GetArgs,
    progress: Arc<ProgressState>,
    reporter: ProgressReporter,
) -> i32 {
    if args.timeout == 0 {
        eprintln!("error: --timeout must be greater than 0");
        return cleanup(handle, task_handle, reporter, None, 1).await;
    }

    // 1. Decode the root index chunk.
    let root = match decode_root_index(&root_data) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: invalid root index chunk: {e}");
            return cleanup(handle, task_handle, reporter, None, 1).await;
        }
    };
    let layout = compute_layout(root.file_size);
    let n = layout.data_chunk_count;
    let tree_depth = layout.depth;

    // Sanity: root.slots should match the canonical layer 0 (data direct) or
    // top-non-root layer (root's children) shape.
    let expected_root_slots: u64 = if tree_depth == 0 {
        n
    } else {
        *layout.layer_counts.last().unwrap()
    };
    if root.slots.len() as u64 != expected_root_slots {
        eprintln!(
            "error: root slot count mismatch: got {}, expected {} (file_size={}, depth={})",
            root.slots.len(),
            expected_root_slots,
            root.file_size,
            tree_depth
        );
        return cleanup(handle, task_handle, reporter, None, 1).await;
    }

    // 2. Update progress state with totals.
    let total_index_chunks = super::tree::total_non_root_index_chunks(root.file_size) + 1;
    progress.set_length(root.file_size, total_index_chunks as u32, n as u32);
    progress.inc_index(); // root accounted for

    // 3. Open output sink.
    let mut output = match open_output_sink(args, root.file_size) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("error: {e}");
            return cleanup(handle, task_handle, reporter, None, 1).await;
        }
    };

    // 4. BFS fetch.
    //
    // `chunk_timeout` is a *sliding* no-progress window: if no chunk has been
    // successfully decoded for this long, the operation aborts. It's *not* a
    // wall-clock budget for the whole download — a steady-progressing fetch
    // can run as long as it needs to.
    //
    // Per-fetch-task deadlines are anchored at spawn time (see
    // `schedule_children_from_index`), so each individual `mutable_get` /
    // `immutable_get` retry loop has up to `chunk_timeout` to land its
    // chunk. Combined with the outer sliding window, this gives:
    //   - Any single chunk: up to chunk_timeout from when its task started.
    //   - Whole operation: aborts only when no progress is being made
    //     anywhere for chunk_timeout.
    let chunk_timeout = Duration::from_secs(args.timeout);
    let sem = Arc::new(Semaphore::new(PARALLEL_FETCH_CAP));
    let mut tasks: JoinSet<TaskOutcome> = JoinSet::new();
    let seen_index = Arc::new(Mutex::new(HashSet::<[u8; 32]>::new()));
    seen_index.lock().await.insert(root_pk);

    // Per-data-position state. Drives need-list publishing and re-spawn
    // of failed fetches; see `ChunkState` for the state machine.
    let mut chunk_states: Vec<ChunkState> = vec![ChunkState::Unscheduled; n as usize];

    // Schedule all of root's children first (or root data slots if depth 0).
    schedule_children_from_index(
        &handle,
        &mut tasks,
        sem.clone(),
        root.slots.clone(),
        tree_depth,
        0,
        n,
        chunk_timeout,
        &mut chunk_states,
    )
    .await;

    // 5. Setup need-list keepalive.
    let need_kp = KeyPair::generate();
    let need_topic_key = need_topic(&root_pk);
    let need_shutdown = Arc::new(tokio::sync::Notify::new());
    let need_announce_handle = tokio::spawn(run_need_announcer(
        handle.clone(),
        need_topic_key,
        need_kp.clone(),
        need_shutdown.clone(),
    ));
    let mut need_seq: u64 = 0;
    let mut last_need_publish = tokio::time::Instant::now();
    let need_publish_interval = Duration::from_secs(20);
    // Skip republishing identical need-list content; keep a single
    // keepalive republish so the DHT record (which expires in ~20m) stays
    // alive even when the missing-set hasn't changed. 10m = half the TTL.
    let mut last_published_encoded: Option<Vec<u8>> = None;
    let mut last_actual_publish_at: Option<tokio::time::Instant> = None;
    let need_keepalive_interval = Duration::from_secs(600);

    // Sliding no-progress window: updated on every successful index/data
    // decode. The drain loop aborts only if this stops moving forward for
    // `chunk_timeout` seconds.
    let mut last_progress_at = tokio::time::Instant::now();

    // 6. Drain results.
    let mut had_error = false;
    while !tasks.is_empty() {
        let outcome = match tokio::time::timeout(Duration::from_secs(1), tasks.join_next()).await {
            Ok(Some(joined)) => match joined {
                Ok(o) => Some(o),
                Err(e) => {
                    eprintln!("  warning: fetch task panicked: {e}");
                    None
                }
            },
            Ok(None) => break,
            Err(_) => None,
        };

        if let Some(outcome) = outcome {
            match outcome {
                TaskOutcome::Index {
                    remaining_depth,
                    base,
                    end,
                    result,
                } => match result {
                    Ok(bytes) => {
                        match decode_non_root_index(&bytes) {
                            Ok(slots) => {
                                progress.inc_index();
                                last_progress_at = tokio::time::Instant::now();
                                let mut seen = seen_index.lock().await;
                                // No-op for loop detection; we already
                                // de-duplicate at schedule time below.
                                let _ = &mut *seen;
                                drop(seen);
                                schedule_children_from_index(
                                    &handle,
                                    &mut tasks,
                                    sem.clone(),
                                    slots,
                                    remaining_depth,
                                    base,
                                    end,
                                    chunk_timeout,
                                    &mut chunk_states,
                                )
                                .await;
                            }
                            Err(e) => {
                                eprintln!(
                                    "error: invalid non-root index at base={base}: {e}"
                                );
                                had_error = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("error: failed to fetch index chunk: {e}");
                        had_error = true;
                        break;
                    }
                },
                TaskOutcome::Data { position, result } => match result {
                    Ok(bytes) => match decode_data_chunk(&bytes) {
                        Ok(payload) => {
                            // Drop late duplicates if this position was
                            // re-spawned and an earlier task also returned.
                            if matches!(chunk_states[position as usize], ChunkState::Done) {
                                continue;
                            }
                            // Trim payload for the last chunk if necessary.
                            let trim_len = if (position + 1) * super::wire::DATA_PAYLOAD_MAX as u64
                                > root.file_size
                            {
                                let already = position * super::wire::DATA_PAYLOAD_MAX as u64;
                                (root.file_size - already) as usize
                            } else {
                                payload.len()
                            };
                            let trimmed = &payload[..trim_len.min(payload.len())];
                            if let Err(e) = output.accept(position, trimmed) {
                                eprintln!("error: {e}");
                                had_error = true;
                                break;
                            }
                            progress.inc_data(trimmed.len() as u64);
                            last_progress_at = tokio::time::Instant::now();
                            chunk_states[position as usize] = ChunkState::Done;
                        }
                        Err(e) => {
                            eprintln!("error: invalid data chunk at position {position}: {e}");
                            had_error = true;
                            break;
                        }
                    },
                    Err(e) => {
                        eprintln!(
                            "  warning: failed to fetch data chunk at position {position}: {e}"
                        );
                        // Transition InFlight → Failed so the next need-list
                        // cycle advertises it and re-spawns a fetch.
                        if let ChunkState::InFlight { address } =
                            chunk_states[position as usize]
                        {
                            chunk_states[position as usize] =
                                ChunkState::Failed { address };
                        }
                    }
                },
            }
        }

        // Periodically publish need-list for chunks the receiver has
        // actually attempted and confirmed missing (Failed state). Chunks
        // that haven't been scheduled yet, or whose fetch is still in
        // flight, are deliberately excluded — we don't want to ask the
        // sender to re-publish what the normal DHT get path may still
        // deliver. The 20s cadence is a batching window so a burst of
        // failures gets coalesced into one need-list update.
        if tokio::time::Instant::now() - last_need_publish >= need_publish_interval {
            let missing: Vec<u32> = chunk_states
                .iter()
                .enumerate()
                .filter_map(|(p, s)| match s {
                    ChunkState::Failed { .. } => Some(p as u32),
                    _ => None,
                })
                .collect();
            if !missing.is_empty() {
                let entries = coalesce_missing_ranges(&missing);
                let encoded = encode_need_list(&entries);
                let unchanged = last_published_encoded.as_deref() == Some(encoded.as_slice());
                let needs_keepalive = last_actual_publish_at
                    .is_none_or(|t| t.elapsed() >= need_keepalive_interval);
                if !unchanged || needs_keepalive {
                    need_seq += 1;
                    let _ = handle.mutable_put(&need_kp, &encoded, need_seq).await;
                    last_actual_publish_at = Some(tokio::time::Instant::now());
                    last_published_encoded = Some(encoded);
                }
                // Re-spawn fetch tasks for Failed positions: with the
                // need-list now published, the sender will republish the
                // missing chunks, and the in-flight retry loop in the new
                // task gets a fresh chunk_timeout window to pick them up.
                for (pos, state) in chunk_states.iter_mut().enumerate() {
                    if let ChunkState::Failed { address } = *state {
                        let h = handle.clone();
                        let permit_sem = sem.clone();
                        tasks.spawn(async move {
                            let _permit = permit_sem.acquire_owned().await.unwrap();
                            let task_deadline =
                                tokio::time::Instant::now() + chunk_timeout;
                            let result =
                                fetch_immutable_with_retry(&h, &address, task_deadline)
                                    .await;
                            TaskOutcome::Data {
                                position: pos as u64,
                                result,
                            }
                        });
                        *state = ChunkState::InFlight { address };
                    }
                }
            }
            last_need_publish = tokio::time::Instant::now();
        }

        // Sliding-window timeout: abort only if no chunk has decoded in
        // the last `chunk_timeout` seconds. Steady-progressing downloads
        // can run as long as they need.
        if tokio::time::Instant::now() - last_progress_at >= chunk_timeout {
            eprintln!(
                "error: no progress for {}s; aborting",
                chunk_timeout.as_secs()
            );
            had_error = true;
            break;
        }
    }

    // 7. Finalize.
    need_shutdown.notify_one();
    let _ = need_announce_handle.await;

    if had_error {
        output.discard();
        return cleanup(handle, task_handle, reporter, Some(need_kp), 1).await;
    }

    // Verify all data positions arrived.
    let done_count = chunk_states
        .iter()
        .filter(|s| matches!(s, ChunkState::Done))
        .count() as u64;
    if done_count != n {
        eprintln!(
            "error: only {} of {} data chunks received",
            done_count, n
        );
        output.discard();
        return cleanup(handle, task_handle, reporter, Some(need_kp), 1).await;
    }

    // CRC verification: read back from output (only meaningful for File mode;
    // streaming stdout has emitted bytes already).
    if let Err(e) = verify_crc(&output, root.file_size, root.crc32c) {
        eprintln!("error: {e}");
        output.discard();
        return cleanup(handle, task_handle, reporter, Some(need_kp), 1).await;
    }

    if let Err(e) = output.finalize() {
        eprintln!("error: {e}");
        return cleanup(handle, task_handle, reporter, Some(need_kp), 1).await;
    }

    // Send empty need-list as the done sentinel, plus an ack.
    need_seq += 1;
    let _ = handle.mutable_put(&need_kp, &[], need_seq).await;
    if !args.no_ack {
        let ack = ack_topic(&root_pk);
        let ack_kp = KeyPair::generate();
        let _ = handle.announce(ack, &ack_kp, &[]).await;
    }

    cleanup(handle, task_handle, reporter, Some(need_kp), 0).await
}

#[allow(clippy::too_many_arguments)]
async fn schedule_children_from_index(
    handle: &HyperDhtHandle,
    tasks: &mut JoinSet<TaskOutcome>,
    sem: Arc<Semaphore>,
    slots: Vec<[u8; 32]>,
    remaining_depth: u32,
    base: u64,
    end: u64,
    chunk_timeout: Duration,
    chunk_states: &mut [ChunkState],
) {
    if remaining_depth == 0 {
        // Slots are data hashes. Position[i] = base + i.
        for (i, address) in slots.into_iter().enumerate() {
            let pos = base + i as u64;
            if pos >= end {
                break;
            }
            chunk_states[pos as usize] = ChunkState::InFlight { address };
            let h = handle.clone();
            let permit_sem = sem.clone();
            tasks.spawn(async move {
                let _permit = permit_sem.acquire_owned().await.unwrap();
                // Per-task deadline anchored at when the task actually
                // starts running, so chunks scheduled mid-fetch get a
                // full budget rather than inheriting the original
                // operation-start deadline.
                let task_deadline = tokio::time::Instant::now() + chunk_timeout;
                let result = fetch_immutable_with_retry(&h, &address, task_deadline).await;
                TaskOutcome::Data {
                    position: pos,
                    result,
                }
            });
        }
        return;
    }

    // Slots are child index pubkeys. Each child covers a subtree.
    // Subtree size at remaining_depth r = NON_ROOT_INDEX_SLOT_CAP^r.
    let child_remaining = remaining_depth - 1;
    let mut subtree_size: u64 = 1;
    for _ in 0..=child_remaining {
        subtree_size = subtree_size.saturating_mul(NON_ROOT_INDEX_SLOT_CAP as u64);
    }

    let mut child_base = base;
    for (i, child_pk) in slots.into_iter().enumerate() {
        if child_base >= end {
            break;
        }
        // Last child of a parent may have a smaller range (due to N being
        // less than the full canonical capacity at this layer). Compute
        // the child's end as min(child_base + subtree_size, end).
        let child_end = (child_base + subtree_size).min(end);
        let h = handle.clone();
        let permit_sem = sem.clone();
        tasks.spawn(async move {
            let _permit = permit_sem.acquire_owned().await.unwrap();
            let task_deadline = tokio::time::Instant::now() + chunk_timeout;
            let result = fetch_mutable_with_retry(&h, &child_pk, task_deadline).await;
            TaskOutcome::Index {
                remaining_depth: child_remaining,
                base: child_base,
                end: child_end,
                result,
            }
        });
        child_base = child_end;
        let _ = i; // suppress unused
    }
}

/// CRC-verify the reassembled output. For File mode, reads the mmap; for
/// Stdout mode, this is a no-op (bytes are downstream already). For empty
/// outputs, verifies that `expected_crc` matches `crc32c(&[])`.
fn verify_crc(output: &OutputSink, file_size: u64, expected_crc: u32) -> Result<(), String> {
    match output {
        OutputSink::File { mmap, .. } => {
            let bytes = &mmap[..file_size as usize];
            let computed = crc32c::crc32c(bytes);
            if computed != expected_crc {
                return Err(format!(
                    "CRC mismatch: expected {expected_crc:08x}, got {computed:08x}"
                ));
            }
        }
        OutputSink::EmptyFile { .. } | OutputSink::EmptyStdout => {
            let computed = crc32c::crc32c(&[]);
            if computed != expected_crc {
                return Err(format!(
                    "CRC mismatch on empty file: expected {expected_crc:08x}, got {computed:08x}"
                ));
            }
        }
        OutputSink::Stdout(_) => {
            // Streaming has already emitted; CRC mismatch is best-effort.
            // We don't recompute (would require buffering the entire file).
        }
    }
    Ok(())
}

/// Cleanup helper: drains DHT handle, awaits the runtime task, finishes the
/// reporter, and returns the exit code.
async fn cleanup(
    handle: HyperDhtHandle,
    task_handle: tokio::task::JoinHandle<
        Result<(), peeroxide_dht::hyperdht::HyperDhtError>,
    >,
    reporter: ProgressReporter,
    _need_kp: Option<KeyPair>,
    code: i32,
) -> i32 {
    reporter.finish().await;
    let _ = handle.destroy().await;
    let _ = task_handle.await;
    code
}

#[cfg(test)]
mod tests {
    // Most fetch.rs logic requires a running DHT; integration tests cover
    // the end-to-end roundtrip in `peeroxide-cli/tests/local_commands.rs`.
    use super::*;

    #[test]
    fn finalize_truncates_mapped_file_to_its_size() {
        let dir = tempfile::tempdir().unwrap();
        let temp_path = dir.path().join("temp");
        let final_path = dir.path().join("out");
        let payload = b"payload shorter than one chunk";

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .unwrap();
        file.set_len(super::super::wire::DATA_PAYLOAD_MAX as u64).unwrap();
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        drop(file);
        mmap[..payload.len()].copy_from_slice(payload);

        OutputSink::File {
            mmap,
            file_size: payload.len() as u64,
            temp_path: temp_path.clone(),
            final_path: final_path.clone(),
        }
        .finalize()
        .unwrap();

        assert_eq!(std::fs::read(&final_path).unwrap(), payload);
        assert!(!temp_path.exists());
    }

    #[test]
    fn finalize_removes_temp_file_when_rename_fails() {
        let dir = tempfile::tempdir().unwrap();
        let temp_path = dir.path().join("temp");
        // A non-empty directory cannot be replaced by a file on any platform.
        let final_path = dir.path().join("out");
        std::fs::create_dir(&final_path).unwrap();
        std::fs::write(final_path.join("keep"), b"x").unwrap();

        let err = OutputSink::EmptyFile {
            temp_path: temp_path.clone(),
            final_path: final_path.clone(),
        }
        .finalize()
        .unwrap_err();

        assert!(err.contains("rename"), "{err}");
        assert!(!temp_path.exists());
        assert!(final_path.join("keep").exists());
    }
}
