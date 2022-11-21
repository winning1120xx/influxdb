#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
// TEMP until everything is fleshed out
#![allow(dead_code)]

//! # WAL
//!
//! This crate provides a local-disk WAL for the IOx ingestion pipeline.

use async_trait::async_trait;
use generated_types::influxdata::{iox::delete::v1::DeletePayload, pbdata::v1::DatabaseBatch};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::{io, path::PathBuf, slice, time::SystemTime};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

mod blocking;

// TODO: Should have more variants / error types to avoid reusing these
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    SegmentFileIdentifierMismatch {},

    UnableToReadFileMetadata {
        source: io::Error,
    },

    UnableToCreateWalDir {
        source: io::Error,
        path: PathBuf,
    },

    UnableToWriteSequenceNumber {
        source: io::Error,
    },

    UnableToWriteChecksum {
        source: io::Error,
    },

    UnableToWriteLength {
        source: io::Error,
    },

    UnableToWriteData {
        source: io::Error,
    },

    UnableToSync {
        source: io::Error,
    },

    UnableToReadDirectoryContents {
        source: io::Error,
        path: PathBuf,
    },

    UnableToOpenFile {
        source: blocking::ReaderError,
        path: PathBuf,
    },

    UnableToSendRequestToReaderTask,

    UnableToReceiveResponseFromSenderTask {
        source: tokio::sync::oneshot::error::RecvError,
    },

    UnableToReadFileHeader {
        source: blocking::ReaderError,
    },

    UnableToReadEntries {
        source: blocking::ReaderError,
    },

    UnableToReadNextOps {
        source: blocking::ReaderError,
    },

    InvalidUuid {
        filename: String,
        source: uuid::Error,
    },
}

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

// todo: change to newtypes
/// SequenceNumber is a u64 monotonically-increasing number provided by users of the WAL for
/// their tracking purposes of data getting written into a segment.
// - who enforces monotonicity? what happens if WAL receives a lower sequence number?
// - this isn't `data_types::SequenceNumber`, should it be or should this be a distinct type
//   because this doesn't have anything to do with Kafka?
// - errr what https://github.com/influxdata/influxdb_iox/blob/4d55efe6558eb09d1ba03a8a5cbfcf48a3425c83/ingester/src/server/grpc/rpc_write.rs#L156-L157
pub type SequenceNumber = u64;

/// Segments are identified by a type 4 UUID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentId(Uuid);

#[allow(missing_docs)]
impl SegmentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn get(&self) -> Uuid {
        self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<Uuid> for SegmentId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for SegmentId {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: find a better name
pub(crate) fn fnamex(dir: impl Into<PathBuf>, id: SegmentId) -> PathBuf {
    let mut path = dir.into();
    path.push(id.to_string());
    path.set_extension(SEGMENT_FILE_EXTENSION);
    path
}

/// The first bytes written into a segment file to identify it and its version.
// TODO: What's the expected way of upgrading -- what happens when we need version 31?
type FileTypeIdentifier = [u8; 8];
const FILE_TYPE_IDENTIFIER: &FileTypeIdentifier = b"INFLUXV3";
/// File extension for segment files.
const SEGMENT_FILE_EXTENSION: &str = "dat";

/// The main type representing one WAL for one ingester instance.
#[derive(Debug)]
pub struct Wal {
    root: PathBuf,
    closed_segments: Vec<ClosedSegment>,
    open_segment: SegmentFile,
}

impl Wal {
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(&root)
            .await
            .context(UnableToCreateWalDirSnafu { path: &root })?;

        let mut dir = tokio::fs::read_dir(&root)
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: &root })?;

        let mut closed_segments = Vec::new();

        while let Some(child) = dir
            .next_entry()
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: &root })?
        {
            let metadata = child
                .metadata()
                .await
                .context(UnableToReadFileMetadataSnafu)?;
            if metadata.is_file() {
                let child_path = child.path();
                let filename = child_path.file_name().unwrap();
                let filename = filename.to_str().unwrap();
                let segment = ClosedSegment {
                    id: Uuid::parse_str(filename)
                        .context(InvalidUuidSnafu { filename })?
                        .into(),
                    path: child.path(),
                    size: metadata.len(),
                    created_at: metadata.created().context(UnableToReadFileMetadataSnafu)?,
                };
                closed_segments.push(segment);
            }
        }

        let open_segment = SegmentFile::new_in_directory(&root).await?;

        Ok(Self {
            root,
            closed_segments,
            open_segment,
        })
    }
}

/// Methods for working with segments (a WAL)
#[async_trait]
pub trait SegmentWal {
    /// Closes the currently open segment and opens a new one, returning the closed segment details
    /// and a handle to the newly opened segment
    async fn rotate(&mut self) -> Result<ClosedSegment>;

    /// Gets a list of the closed segments
    fn closed_segments(&self) -> &[ClosedSegment];

    /// Opens a reader for a given segment from the WAL
    async fn reader_for_segment(&self, id: SegmentId) -> Result<SegmentFileReader>;

    /// Returns a handle to the WAL to commit entries to the currently active segment.
    async fn write_handle(&self) -> OpenSegment;

    /// Deletes the segment from storage
    async fn delete_segment(&self, id: SegmentId) -> Result<()>;
}

#[async_trait]
impl SegmentWal for Wal {
    async fn rotate(&mut self) -> Result<ClosedSegment> {
        let closed = self.open_segment.rotate().await?;
        self.closed_segments.push(closed.clone());
        Ok(closed)
    }

    fn closed_segments(&self) -> &[ClosedSegment] {
        &self.closed_segments
    }

    async fn reader_for_segment(&self, id: SegmentId) -> Result<SegmentFileReader> {
        let path = fnamex(&self.root, id);
        SegmentFileReader::from_path(path).await
    }

    async fn write_handle(&self) -> OpenSegment {
        self.open_segment.handle()
    }

    async fn delete_segment(&self, _id: SegmentId) -> Result<()> {
        todo!();
    }
}

/// Operation recorded in the WAL
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum WalOp {
    Write(DatabaseBatch),
    Delete(DeletePayload),
    Persist(PersistOp),
}

/// WAL operation with a sequence number, which is used to inform read buffers when to evict data
/// from the buffer
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SequencedWalOp {
    pub sequence_number: SequenceNumber,
    pub op: WalOp,
}

/// Serializable and deserializable persist information that can be saved to the WAL. This is
/// used during replay to evict data from memory.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct PersistOp {
    // todo: use data_types for these
    namespace_id: i64,
    table_id: i64,
    partition_id: i64,
    parquet_file_uuid: String,
}

/// Methods for reading ops from a segment in a WAL
#[async_trait]
pub trait OpReader {
    // get the next collection of ops. Since ops are batched into Segments, they come
    // back as a collection. Each `SegmentEntry` will encode a `Vec<SequencedWalOp>`.
    // todo: change to Result<Vec<SequencedWalOp>>, or a stream of `Vec<SequencedWalOp>`s?
    async fn next(&mut self) -> Result<Option<Vec<SequencedWalOp>>>;
}

/// Methods for a `Segment`
#[async_trait]
pub trait Segment {
    /// Get the id of the segment
    fn id(&self) -> SegmentId;

    /// Persist an operation into the segment. The `Segment` trait implementer is meant to be an
    /// accumulator that will batch ops together, and `write_op` calls will return when the
    /// collection has been persisted to the segment file.
    async fn write_op(&self, op: &SequencedWalOp) -> Result<WriteSummary>;

    /// Return a reader for the ops in the segment
    async fn reader(&self) -> Result<Box<dyn OpReader>>;
}

/// Raw, uncompressed and unstructured data for a Segment entry with a checksum.
#[derive(Debug, Eq, PartialEq)]
pub struct SegmentEntry {
    /// The CRC checksum of the uncompressed data
    pub checksum: u32,
    /// The uncompressed data
    pub data: Vec<u8>,
}

/// Summary information after a write
#[derive(Debug, Copy, Clone)]
pub struct WriteSummary {
    /// Total size of the segment in bytes
    pub total_bytes: usize,
    /// Number of bytes written to segment in this write
    pub bytes_written: usize,
    /// Checksum for the compressed data written to segment
    pub checksum: u32,
}

#[derive(Debug)]
enum SegmentFileWriterRequest {
    Write(oneshot::Sender<WriteSummary>, Vec<u8>), // todo Bytes
    Rotate(oneshot::Sender<ClosedSegment>, ()),
}

/// A Segment in a WAL. One segment is stored in one file.
#[derive(Debug)]
struct SegmentFile {
    tx: mpsc::Sender<SegmentFileWriterRequest>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl SegmentFile {
    async fn new_in_directory(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let (tx, rx) = mpsc::channel(10);
        let task = tokio::task::spawn_blocking(|| Self::task_main(rx, dir));
        Ok(Self { tx, task })
    }

    fn task_main(
        mut rx: tokio::sync::mpsc::Receiver<SegmentFileWriterRequest>,
        dir: PathBuf,
    ) -> Result<()> {
        let new_writ = || Ok(blocking::SegmentFileWriter::new_in_directory(&dir).unwrap());
        let mut open_write = new_writ()?;

        while let Some(req) = rx.blocking_recv() {
            use SegmentFileWriterRequest::*;

            match req {
                Write(tx, data) => {
                    let x = open_write.write(&data).unwrap();
                    tx.send(x).unwrap();
                }

                Rotate(tx, ()) => {
                    let old = std::mem::replace(&mut open_write, new_writ()?);
                    let res = old.close().unwrap();
                    tx.send(res).unwrap();
                }
            }
        }

        Ok(())
    }

    async fn one_command<Req, Resp, Args>(
        tx: &mpsc::Sender<SegmentFileWriterRequest>,
        req: Req,
        args: Args,
    ) -> Result<Resp>
    where
        Req: FnOnce(oneshot::Sender<Resp>, Args) -> SegmentFileWriterRequest,
    {
        let (req_tx, req_rx) = oneshot::channel();

        tx.send(req(req_tx, args)).await.unwrap();
        Ok(req_rx.await.unwrap())
    }

    fn handle(&self) -> OpenSegment {
        OpenSegment(self.tx.clone())
    }

    async fn write(&self, data: &[u8]) -> Result<WriteSummary> {
        Self::one_command(&self.tx, SegmentFileWriterRequest::Write, data.to_vec()).await
    }

    async fn write_ops(&self, ops: &[SequencedWalOp]) -> Result<WriteSummary> {
        // todo: bincode instead of serde_json
        let encoded = serde_json::to_vec(&ops).unwrap();
        self.write(&encoded).await
    }

    async fn rotate(&self) -> Result<ClosedSegment> {
        Self::one_command(&self.tx, SegmentFileWriterRequest::Rotate, ()).await
    }
}

/// Handle to the one currently open segment for users of the WAL to send [`SequencedWalOp`]s to.
#[derive(Debug)]
pub struct OpenSegment(mpsc::Sender<SegmentFileWriterRequest>);

impl OpenSegment {
    async fn write(&self, data: &[u8]) -> Result<WriteSummary> {
        SegmentFile::one_command(&self.0, SegmentFileWriterRequest::Write, data.to_vec()).await
    }

    pub async fn write_op(&self, op: &SequencedWalOp) -> Result<WriteSummary> {
        // todo: bincode instead of serde_json
        let ops = slice::from_ref(op);
        let encoded = serde_json::to_vec(&ops).unwrap();
        self.write(&encoded).await
    }
}

#[derive(Debug)]
enum SegmentFileReaderRequest {
    ReadHeader(oneshot::Sender<blocking::ReaderResult<(FileTypeIdentifier, uuid::Bytes)>>),

    Entries(oneshot::Sender<blocking::ReaderResult<Vec<SegmentEntry>>>),

    NextOps(oneshot::Sender<blocking::ReaderResult<Option<Vec<SequencedWalOp>>>>),
}

/// Enables reading a particular closed segment's entries.
#[derive(Debug)]
pub struct SegmentFileReader {
    id: SegmentId,
    tx: mpsc::Sender<SegmentFileReaderRequest>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl SegmentFileReader {
    async fn from_path(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        let (tx, rx) = mpsc::channel::<SegmentFileReaderRequest>(10);
        let task = tokio::task::spawn_blocking(|| Self::task_main(rx, path));

        let (file_type, id) = Self::one_command(&tx, SegmentFileReaderRequest::ReadHeader)
            .await?
            .context(UnableToReadFileHeaderSnafu)?;

        ensure!(
            &file_type == FILE_TYPE_IDENTIFIER,
            SegmentFileIdentifierMismatchSnafu,
        );

        let id = Uuid::from_bytes(id);
        let id = SegmentId::from(id);

        Ok(Self { id, tx, task })
    }

    fn task_main(mut rx: mpsc::Receiver<SegmentFileReaderRequest>, path: PathBuf) -> Result<()> {
        let mut reader = blocking::SegmentFileReader::from_path(&path)
            .context(UnableToOpenFileSnafu { path })?;

        while let Some(req) = rx.blocking_recv() {
            use SegmentFileReaderRequest::*;

            // We don't care if we can't respond to the request.
            match req {
                ReadHeader(tx) => {
                    tx.send(reader.read_header()).ok();
                }

                Entries(tx) => {
                    tx.send(reader.entries()).ok();
                }

                NextOps(tx) => {
                    tx.send(reader.next_ops()).ok();
                }
            };
        }

        Ok(())
    }

    async fn one_command<Req, Resp>(
        tx: &mpsc::Sender<SegmentFileReaderRequest>,
        req: Req,
    ) -> Result<Resp>
    where
        Req: FnOnce(oneshot::Sender<Resp>) -> SegmentFileReaderRequest,
    {
        let (req_tx, req_rx) = oneshot::channel();
        tx.send(req(req_tx))
            .await
            .ok()
            .context(UnableToSendRequestToReaderTaskSnafu)?;
        req_rx
            .await
            .context(UnableToReceiveResponseFromSenderTaskSnafu)
    }

    // TODO: Should this return a stream instead of a big vector?
    async fn entries(&mut self) -> Result<Vec<SegmentEntry>> {
        Self::one_command(&self.tx, SegmentFileReaderRequest::Entries)
            .await?
            .context(UnableToReadEntriesSnafu)
    }

    pub async fn next_ops(&mut self) -> Result<Option<Vec<SequencedWalOp>>> {
        Self::one_command(&self.tx, SegmentFileReaderRequest::NextOps)
            .await?
            .context(UnableToReadNextOpsSnafu)
    }
}

/// Metadata for a WAL segment that is no longer accepting writes, but can be read for replay
/// purposes.
#[derive(Debug, Clone)]
pub struct ClosedSegment {
    id: SegmentId,
    path: PathBuf,
    size: u64,
    created_at: SystemTime,
}

impl ClosedSegment {
    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{NamespaceId, TableId};
    use dml::DmlWrite;
    use mutable_batch_lp::lines_to_batches;

    #[tokio::test]
    async fn segment_file_write_and_read_entries() {
        let dir = test_helpers::tmp_dir().unwrap();
        let sf = SegmentFile::new_in_directory(dir.path()).await.unwrap();

        let data = b"whatevs";
        let write_summary = sf.write(data).await.unwrap();

        let data2 = b"another";
        let summary2 = sf.write(data2).await.unwrap();

        let closed = sf.rotate().await.unwrap();

        let mut reader = SegmentFileReader::from_path(&closed.path).await.unwrap();
        let entries = reader.entries().await.unwrap();
        assert_eq!(
            &entries,
            &[
                SegmentEntry {
                    checksum: write_summary.checksum,
                    data: data.to_vec(),
                },
                SegmentEntry {
                    checksum: summary2.checksum,
                    data: data2.to_vec()
                },
            ]
        );
    }

    #[tokio::test]
    async fn segment_file_write_and_read_ops() {
        let dir = test_helpers::tmp_dir().unwrap();
        let segment = SegmentFile::new_in_directory(dir.path()).await.unwrap();

        let w1 = test_data("m1,t=foo v=1i 1");
        let w2 = test_data("m1,t=foo v=2i 2");

        let op1 = SequencedWalOp {
            sequence_number: 0,
            op: WalOp::Write(w1),
        };
        let op2 = SequencedWalOp {
            sequence_number: 1,
            op: WalOp::Write(w2),
        };

        let ops = vec![op1, op2];
        segment.write_ops(&ops).await.unwrap();

        let closed = segment.rotate().await.unwrap();

        let mut reader = SegmentFileReader::from_path(&closed.path).await.unwrap();
        let read_ops = reader.next_ops().await.unwrap().unwrap();
        assert_eq!(ops, read_ops);
    }

    // test delete and persist ops

    // open wal and write and read ops from segment

    // rotates wal to new segment

    // open wal and get closed segments and read them

    // delete segment

    // open wal with files that aren't segments (should log and skip)

    // read segment works even if last entry is truncated

    // writes get batched

    fn test_data(lp: &str) -> DatabaseBatch {
        let batches = lines_to_batches(lp, 0).unwrap();
        let batches = batches
            .into_iter()
            .enumerate()
            .map(|(i, (_table_name, batch))| (TableId::new(i as _), batch))
            .collect();

        let write = DmlWrite::new(
            NamespaceId::new(42),
            batches,
            "bananas".into(),
            Default::default(),
        );

        mutable_batch_pb::encode::encode_write(42, &write)
    }
}
