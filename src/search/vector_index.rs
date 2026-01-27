//! CVVI (Cass Vector Index) binary format definitions.
//!
//! Format overview (little-endian):
//!
//! Header (variable size):
//!   Magic: "CVVI" (4 bytes)
//!   Version: u16
//!   EmbedderID length: u16
//!   EmbedderID: bytes
//!   EmbedderRevision length: u16
//!   EmbedderRevision: bytes
//!   Dimension: u32
//!   Quantization: u8 (0=f32, 1=f16)
//!   Count: u32
//!   HeaderCRC32: u32 (CRC32 of header bytes before this field)
//!
//! Rows (fixed size per entry):
//!   MessageID: u64
//!   CreatedAtMs: i64
//!   AgentID: u32
//!   WorkspaceID: u32
//!   SourceID: u32
//!   Role: u8 (0=user, 1=assistant, 2=system, 3=tool)
//!   ChunkIdx: u8 (0 for single-chunk)
//!   VecOffset: u64 (offset into vector slab)
//!   ContentHash: [u8; 32] (SHA256 of canonical content)
//!
//! Vector slab:
//!   Count × Dimension × bytes_per_quant, contiguous, 32-byte aligned.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use half::f16;
use memmap2::Mmap;
use rayon::prelude::*;
use rusqlite::Connection;

use crate::search::query::SearchFilters;
use crate::sources::provenance::{LOCAL_SOURCE_ID, SourceFilter, SourceKind};
use crate::storage::sqlite::SqliteStorage;

pub const CVVI_MAGIC: [u8; 4] = *b"CVVI";
pub const CVVI_VERSION: u16 = 1;
pub const VECTOR_ALIGN_BYTES: usize = 32;
pub const ROW_SIZE_BYTES: usize = 70;
pub const VECTOR_INDEX_DIR: &str = "vector_index";

/// P1 Opt 3: Minimum vector count for parallel search.
/// Below this threshold, Rayon overhead (~1-5µs per task) outweighs parallelism benefit.
const PARALLEL_THRESHOLD: usize = 10_000;

/// P1 Opt 3: Chunk size for parallel iteration.
/// Smaller chunks = better load balancing but more overhead. 1024 is a good default.
const PARALLEL_CHUNK_SIZE: usize = 1024;

/// Cached parallel search enable flag (checked once at first use).
/// Set CASS_PARALLEL_SEARCH=0 to disable parallel search.
static PARALLEL_SEARCH_ENABLED: once_cell::sync::Lazy<bool> = once_cell::sync::Lazy::new(|| {
    dotenvy::var("CASS_PARALLEL_SEARCH")
        .map(|v| v != "0" && v.to_lowercase() != "false")
        .unwrap_or(true)
});

pub fn vector_index_path(data_dir: &Path, embedder_id: &str) -> PathBuf {
    data_dir
        .join(VECTOR_INDEX_DIR)
        .join(format!("index-{embedder_id}.cvvi"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantization {
    F32,
    F16,
}

impl Quantization {
    pub fn to_u8(self) -> u8 {
        match self {
            Quantization::F32 => 0,
            Quantization::F16 => 1,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Quantization::F32),
            1 => Ok(Quantization::F16),
            other => bail!("unknown quantization value: {other}"),
        }
    }

    pub fn bytes_per_component(self) -> usize {
        match self {
            Quantization::F32 => 4,
            Quantization::F16 => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CvviHeader {
    pub version: u16,
    pub embedder_id: String,
    pub embedder_revision: String,
    pub dimension: u32,
    pub quantization: Quantization,
    pub count: u32,
}

impl CvviHeader {
    pub fn new(
        embedder_id: impl Into<String>,
        embedder_revision: impl Into<String>,
        dimension: u32,
        quantization: Quantization,
        count: u32,
    ) -> Result<Self> {
        let header = Self {
            version: CVVI_VERSION,
            embedder_id: embedder_id.into(),
            embedder_revision: embedder_revision.into(),
            dimension,
            quantization,
            count,
        };
        header.validate()?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<()> {
        let id_len = self.embedder_id.len();
        let rev_len = self.embedder_revision.len();
        if id_len > u16::MAX as usize {
            bail!("embedder_id is too long: {id_len}");
        }
        if rev_len > u16::MAX as usize {
            bail!("embedder_revision is too long: {rev_len}");
        }
        if self.dimension == 0 {
            bail!("dimension must be non-zero");
        }
        Ok(())
    }

    pub fn header_len_bytes(&self) -> Result<usize> {
        self.validate()?;
        let id_len = self.embedder_id.len();
        let rev_len = self.embedder_revision.len();
        let base = 4 + 2 + 2 + id_len + 2 + rev_len + 4 + 1 + 4 + 4;
        Ok(base)
    }

    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<usize> {
        self.validate()?;
        let mut buf = Vec::new();

        buf.extend_from_slice(&CVVI_MAGIC);
        buf.extend_from_slice(&self.version.to_le_bytes());

        let id_bytes = self.embedder_id.as_bytes();
        let id_len = u16::try_from(id_bytes.len())
            .map_err(|_| anyhow!("embedder_id length out of range"))?;
        buf.extend_from_slice(&id_len.to_le_bytes());
        buf.extend_from_slice(id_bytes);

        let rev_bytes = self.embedder_revision.as_bytes();
        let rev_len = u16::try_from(rev_bytes.len())
            .map_err(|_| anyhow!("embedder_revision length out of range"))?;
        buf.extend_from_slice(&rev_len.to_le_bytes());
        buf.extend_from_slice(rev_bytes);

        buf.extend_from_slice(&self.dimension.to_le_bytes());
        buf.push(self.quantization.to_u8());
        buf.extend_from_slice(&self.count.to_le_bytes());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();

        writer.write_all(&buf)?;
        writer.write_all(&crc.to_le_bytes())?;
        Ok(buf.len() + 4)
    }

    pub fn read_from<R: Read>(mut reader: R) -> Result<Self> {
        let mut header_bytes = Vec::new();

        let magic =
            read_exact_array::<4, _>(&mut reader, &mut header_bytes).context("read CVVI magic")?;
        if magic != CVVI_MAGIC {
            bail!("invalid CVVI magic: {:?}", magic);
        }

        let version = read_u16_le(&mut reader, &mut header_bytes).context("read CVVI version")?;
        if version != CVVI_VERSION {
            bail!("unsupported CVVI version: {version}");
        }

        let id_len = read_u16_le(&mut reader, &mut header_bytes)
            .context("read embedder id length")? as usize;
        let id_bytes =
            read_exact_vec(&mut reader, id_len, &mut header_bytes).context("read embedder id")?;
        let embedder_id = String::from_utf8(id_bytes).context("embedder id is not valid UTF-8")?;

        let rev_len = read_u16_le(&mut reader, &mut header_bytes)
            .context("read embedder revision length")? as usize;
        let rev_bytes = read_exact_vec(&mut reader, rev_len, &mut header_bytes)
            .context("read embedder revision")?;
        let embedder_revision =
            String::from_utf8(rev_bytes).context("embedder revision is not valid UTF-8")?;

        let dimension = read_u32_le(&mut reader, &mut header_bytes).context("read dimension")?;
        let quantization_raw =
            read_u8(&mut reader, &mut header_bytes).context("read quantization")?;
        let quantization = Quantization::from_u8(quantization_raw)?;
        let count = read_u32_le(&mut reader, &mut header_bytes).context("read count")?;

        let crc_expected = read_u32_le_no_accum(&mut reader).context("read header crc")?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_bytes);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            bail!("header CRC mismatch (expected {crc_expected:#010x}, got {crc_actual:#010x})");
        }

        let header = Self {
            version,
            embedder_id,
            embedder_revision,
            dimension,
            quantization,
            count,
        };
        header.validate()?;
        Ok(header)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRow {
    pub message_id: u64,
    pub created_at_ms: i64,
    pub agent_id: u32,
    pub workspace_id: u32,
    pub source_id: u32,
    pub role: u8,
    pub chunk_idx: u8,
    pub vec_offset: u64,
    pub content_hash: [u8; 32],
}

impl VectorRow {
    pub fn to_bytes(&self) -> [u8; ROW_SIZE_BYTES] {
        let mut buf = [0u8; ROW_SIZE_BYTES];
        let mut offset = 0usize;

        buf[offset..offset + 8].copy_from_slice(&self.message_id.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&self.created_at_ms.to_le_bytes());
        offset += 8;
        buf[offset..offset + 4].copy_from_slice(&self.agent_id.to_le_bytes());
        offset += 4;
        buf[offset..offset + 4].copy_from_slice(&self.workspace_id.to_le_bytes());
        offset += 4;
        buf[offset..offset + 4].copy_from_slice(&self.source_id.to_le_bytes());
        offset += 4;
        buf[offset] = self.role;
        offset += 1;
        buf[offset] = self.chunk_idx;
        offset += 1;
        buf[offset..offset + 8].copy_from_slice(&self.vec_offset.to_le_bytes());
        offset += 8;
        buf[offset..offset + 32].copy_from_slice(&self.content_hash);

        buf
    }

    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() != ROW_SIZE_BYTES {
            bail!(
                "vector row size mismatch: expected {ROW_SIZE_BYTES}, got {}",
                buf.len()
            );
        }
        let mut offset = 0usize;
        let message_id = u64::from_le_bytes(buf[offset..offset + 8].try_into()?);
        offset += 8;
        let created_at_ms = i64::from_le_bytes(buf[offset..offset + 8].try_into()?);
        offset += 8;
        let agent_id = u32::from_le_bytes(buf[offset..offset + 4].try_into()?);
        offset += 4;
        let workspace_id = u32::from_le_bytes(buf[offset..offset + 4].try_into()?);
        offset += 4;
        let source_id = u32::from_le_bytes(buf[offset..offset + 4].try_into()?);
        offset += 4;
        let role = buf[offset];
        offset += 1;
        let chunk_idx = buf[offset];
        offset += 1;
        let vec_offset = u64::from_le_bytes(buf[offset..offset + 8].try_into()?);
        offset += 8;
        let content_hash = buf[offset..offset + 32].try_into()?;

        Ok(Self {
            message_id,
            created_at_ms,
            agent_id,
            workspace_id,
            source_id,
            role,
            chunk_idx,
            vec_offset,
            content_hash,
        })
    }

    pub fn read_from<R: Read>(mut reader: R) -> Result<Self> {
        let mut buf = [0u8; ROW_SIZE_BYTES];
        reader.read_exact(&mut buf)?;
        Self::from_bytes(&buf)
    }
}

#[derive(Debug, Clone)]
pub struct VectorEntry {
    pub message_id: u64,
    pub created_at_ms: i64,
    pub agent_id: u32,
    pub workspace_id: u32,
    pub source_id: u32,
    pub role: u8,
    pub chunk_idx: u8,
    pub content_hash: [u8; 32],
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, Default)]
pub struct SemanticFilter {
    pub agents: Option<HashSet<u32>>,
    pub workspaces: Option<HashSet<u32>>,
    pub sources: Option<HashSet<u32>>,
    pub roles: Option<HashSet<u8>>,
    pub created_from: Option<i64>,
    pub created_to: Option<i64>,
}

impl SemanticFilter {
    pub fn matches(&self, row: &VectorRow) -> bool {
        if let Some(agents) = &self.agents
            && !agents.contains(&row.agent_id)
        {
            return false;
        }
        if let Some(workspaces) = &self.workspaces
            && !workspaces.contains(&row.workspace_id)
        {
            return false;
        }
        if let Some(sources) = &self.sources
            && !sources.contains(&row.source_id)
        {
            return false;
        }
        if let Some(roles) = &self.roles
            && !roles.contains(&row.role)
        {
            return false;
        }
        if let Some(from) = self.created_from
            && row.created_at_ms < from
        {
            return false;
        }
        if let Some(to) = self.created_to
            && row.created_at_ms > to
        {
            return false;
        }
        true
    }

    pub fn from_search_filters(filters: &SearchFilters, maps: &SemanticFilterMaps) -> Result<Self> {
        let agents = map_filter_set(&filters.agents, &maps.agent_slug_to_id);
        let workspaces = map_filter_set(&filters.workspaces, &maps.workspace_path_to_id);
        let sources = maps.sources_from_filter(&filters.source_filter)?;

        Ok(Self {
            agents,
            workspaces,
            sources,
            roles: None,
            created_from: filters.created_from,
            created_to: filters.created_to,
        })
    }

    pub fn with_roles(mut self, roles: Option<HashSet<u8>>) -> Self {
        self.roles = roles;
        self
    }
}

pub const ROLE_USER: u8 = 0;
pub const ROLE_ASSISTANT: u8 = 1;
pub const ROLE_SYSTEM: u8 = 2;
pub const ROLE_TOOL: u8 = 3;

pub fn role_code_from_str(role: &str) -> Option<u8> {
    match role.trim().to_lowercase().as_str() {
        "user" => Some(ROLE_USER),
        "assistant" | "agent" => Some(ROLE_ASSISTANT),
        "system" => Some(ROLE_SYSTEM),
        "tool" => Some(ROLE_TOOL),
        _ => None,
    }
}

pub fn parse_role_codes<I, S>(roles: I) -> Result<HashSet<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut set = HashSet::new();
    for role in roles {
        let role_str = role.as_ref();
        let code =
            role_code_from_str(role_str).ok_or_else(|| anyhow!("unknown role: {role_str}"))?;
        set.insert(code);
    }
    Ok(set)
}

#[derive(Debug, Clone)]
pub struct SemanticFilterMaps {
    agent_slug_to_id: HashMap<String, u32>,
    workspace_path_to_id: HashMap<String, u32>,
    source_id_to_id: HashMap<String, u32>,
    remote_source_ids: HashSet<u32>,
}

impl SemanticFilterMaps {
    pub fn from_storage(storage: &SqliteStorage) -> Result<Self> {
        Self::from_connection(storage.raw())
    }

    pub fn from_connection(conn: &Connection) -> Result<Self> {
        let mut agent_slug_to_id = HashMap::new();
        let mut stmt = conn.prepare("SELECT id, slug FROM agents")?;
        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let slug: String = row.get(1)?;
            Ok((id, slug))
        })?;
        for row in rows {
            let (id, slug) = row?;
            let id_u32 = u32::try_from(id).map_err(|_| anyhow!("agent id out of range"))?;
            agent_slug_to_id.insert(slug, id_u32);
        }

        let mut workspace_path_to_id = HashMap::new();
        let mut stmt = conn.prepare("SELECT id, path FROM workspaces")?;
        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let path: String = row.get(1)?;
            Ok((id, path))
        })?;
        for row in rows {
            let (id, path) = row?;
            let id_u32 = u32::try_from(id).map_err(|_| anyhow!("workspace id out of range"))?;
            workspace_path_to_id.insert(path, id_u32);
        }

        let mut source_id_to_id = HashMap::new();
        let mut remote_source_ids = HashSet::new();
        let mut stmt = conn.prepare("SELECT id, kind FROM sources")?;
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let kind: String = row.get(1)?;
            Ok((id, kind))
        })?;
        for row in rows {
            let (id, kind) = row?;
            let id_u32 = source_id_hash(&id);
            if SourceKind::parse(&kind).is_none_or(|k| k.is_remote()) {
                remote_source_ids.insert(id_u32);
            }
            source_id_to_id.insert(id, id_u32);
        }

        Ok(Self {
            agent_slug_to_id,
            workspace_path_to_id,
            source_id_to_id,
            remote_source_ids,
        })
    }

    fn sources_from_filter(&self, filter: &SourceFilter) -> Result<Option<HashSet<u32>>> {
        let result = match filter {
            SourceFilter::All => None,
            SourceFilter::Local => Some(HashSet::from([self.source_id(LOCAL_SOURCE_ID)])),
            SourceFilter::Remote => Some(self.remote_source_ids.clone()),
            SourceFilter::SourceId(id) => Some(HashSet::from([self.source_id(id)])),
        };
        Ok(result)
    }

    fn source_id(&self, source_id: &str) -> u32 {
        self.source_id_to_id
            .get(source_id)
            .copied()
            .unwrap_or_else(|| source_id_hash(source_id))
    }
}

#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub message_id: u64,
    pub chunk_idx: u8,
    pub score: f32,
}

#[derive(Debug)]
pub struct VectorIndex {
    header: CvviHeader,
    rows: Vec<VectorRow>,
    vectors: VectorStorage,
}

#[derive(Debug)]
enum VectorStorage {
    F32(Vec<f32>),
    F16(Vec<f16>),
    /// P0 Opt 1: F32 data pre-converted from F16 at load time.
    /// The vec_offset values are still in F16 byte terms (2 bytes per component),
    /// so we use 2 as the divisor when computing element indices.
    PreconvertedF32(Vec<f32>),
    Mmap {
        mmap: Mmap,
        offset: usize,
        len: usize,
    },
}

impl VectorIndex {
    pub fn build<I>(
        embedder_id: impl Into<String>,
        embedder_revision: impl Into<String>,
        dimension: usize,
        quantization: Quantization,
        entries: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = VectorEntry>,
    {
        if dimension == 0 {
            bail!("dimension must be non-zero");
        }
        let dimension_u32 =
            u32::try_from(dimension).map_err(|_| anyhow!("dimension out of range"))?;

        let entries: Vec<VectorEntry> = entries.into_iter().collect();
        let count_u32 =
            u32::try_from(entries.len()).map_err(|_| anyhow!("entry count out of range"))?;

        let mut rows = Vec::with_capacity(entries.len());
        let mut offset_bytes: usize = 0;
        let bytes_per = quantization.bytes_per_component();
        let vector_bytes = dimension
            .checked_mul(bytes_per)
            .ok_or_else(|| anyhow!("vector size overflow"))?;

        let vectors = match quantization {
            Quantization::F32 => {
                let mut slab = Vec::with_capacity(entries.len() * dimension);
                for entry in &entries {
                    if entry.vector.len() != dimension {
                        bail!(
                            "vector dimension mismatch: expected {}, got {}",
                            dimension,
                            entry.vector.len()
                        );
                    }
                    let vec_offset = u64::try_from(offset_bytes)
                        .map_err(|_| anyhow!("vector offset out of range"))?;
                    rows.push(VectorRow {
                        message_id: entry.message_id,
                        created_at_ms: entry.created_at_ms,
                        agent_id: entry.agent_id,
                        workspace_id: entry.workspace_id,
                        source_id: entry.source_id,
                        role: entry.role,
                        chunk_idx: entry.chunk_idx,
                        vec_offset,
                        content_hash: entry.content_hash,
                    });
                    slab.extend(entry.vector.iter().copied());
                    offset_bytes = offset_bytes
                        .checked_add(vector_bytes)
                        .ok_or_else(|| anyhow!("vector slab size overflow"))?;
                }
                VectorStorage::F32(slab)
            }
            Quantization::F16 => {
                let mut slab = Vec::with_capacity(entries.len() * dimension);
                for entry in &entries {
                    if entry.vector.len() != dimension {
                        bail!(
                            "vector dimension mismatch: expected {}, got {}",
                            dimension,
                            entry.vector.len()
                        );
                    }
                    let vec_offset = u64::try_from(offset_bytes)
                        .map_err(|_| anyhow!("vector offset out of range"))?;
                    rows.push(VectorRow {
                        message_id: entry.message_id,
                        created_at_ms: entry.created_at_ms,
                        agent_id: entry.agent_id,
                        workspace_id: entry.workspace_id,
                        source_id: entry.source_id,
                        role: entry.role,
                        chunk_idx: entry.chunk_idx,
                        vec_offset,
                        content_hash: entry.content_hash,
                    });
                    slab.extend(entry.vector.iter().map(|v| f16::from_f32(*v)));
                    offset_bytes = offset_bytes
                        .checked_add(vector_bytes)
                        .ok_or_else(|| anyhow!("vector slab size overflow"))?;
                }
                VectorStorage::F16(slab)
            }
        };

        let header = CvviHeader::new(
            embedder_id,
            embedder_revision,
            dimension_u32,
            quantization,
            count_u32,
        )?;

        let index = Self {
            header,
            rows,
            vectors,
        };
        index.validate()?;
        Ok(index)
    }

    pub fn load(path: &Path) -> Result<Self> {
        if cfg!(target_endian = "big") {
            bail!("CVVI load is only supported on little-endian targets");
        }

        let file = File::open(path).with_context(|| format!("open CVVI file {path:?}"))?;
        let metadata = file.metadata().context("read CVVI metadata")?;
        let file_len = metadata.len();
        if file_len == 0 {
            bail!("CVVI file is empty");
        }

        let mmap = unsafe { Mmap::map(&file).context("mmap CVVI file")? };
        let mut cursor = Cursor::new(&mmap[..]);
        let header = CvviHeader::read_from(&mut cursor).context("read CVVI header")?;
        let header_len = header.header_len_bytes()?;
        let rows_len = rows_size_bytes(header.count)?;
        let slab_offset = vector_slab_offset_bytes(header_len, header.count)?;
        let slab_size =
            vector_slab_size_bytes(header.count, header.dimension, header.quantization)?;

        let expected_len = slab_offset
            .checked_add(slab_size)
            .ok_or_else(|| anyhow!("CVVI file size overflow"))?;
        if file_len != expected_len as u64 {
            bail!(
                "CVVI file size mismatch (expected {}, got {})",
                expected_len,
                file_len
            );
        }

        let rows_start = header_len;
        let rows_end = rows_start
            .checked_add(rows_len)
            .ok_or_else(|| anyhow!("rows offset overflow"))?;
        let rows_bytes = mmap
            .get(rows_start..rows_end)
            .ok_or_else(|| anyhow!("rows out of bounds"))?;
        let mut rows = Vec::with_capacity(header.count as usize);
        for chunk in rows_bytes.chunks_exact(ROW_SIZE_BYTES) {
            rows.push(VectorRow::from_bytes(chunk)?);
        }
        if rows.len() != header.count as usize {
            bail!(
                "row count mismatch: expected {}, got {}",
                header.count,
                rows.len()
            );
        }

        validate_row_offsets(
            &rows,
            header.dimension as usize,
            header.quantization,
            slab_size,
        )?;

        // P0 Opt 1: Pre-convert F16→F32 at load time to eliminate per-query conversion.
        // Env var CASS_F16_PRECONVERT=0 disables this (keeps mmap + lazy conversion).
        let f16_preconvert_enabled = dotenvy::var("CASS_F16_PRECONVERT")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true);

        let vectors = if f16_preconvert_enabled && header.quantization == Quantization::F16 {
            // Pre-convert entire F16 slab to F32 for faster dot products.
            // Trade-off: 2x memory usage, but eliminates 19.2M conversions/query for 50k vectors.
            // Bench (search_perf::vector_index_search_50k_loaded, 2026-01-11):
            // CASS_F16_PRECONVERT=0 → ~4.57ms; default → ~1.83ms (~60% faster).
            let slab_end = slab_offset
                .checked_add(slab_size)
                .ok_or_else(|| anyhow!("slab offset overflow"))?;
            let slab_bytes = mmap
                .get(slab_offset..slab_end)
                .ok_or_else(|| anyhow!("slab out of bounds"))?;
            let f16_slice = bytes_as_f16(slab_bytes)?;
            let f32_slab: Vec<f32> = f16_slice.iter().map(|v| f32::from(*v)).collect();
            VectorStorage::PreconvertedF32(f32_slab)
        } else {
            VectorStorage::Mmap {
                mmap,
                offset: slab_offset,
                len: slab_size,
            }
        };

        let index = Self {
            header,
            rows,
            vectors,
        };
        index.validate()?;
        Ok(index)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let temp_path = path.with_extension("cvvi.tmp");
        let mut file = File::create(&temp_path)
            .with_context(|| format!("create temp CVVI file {temp_path:?}"))?;
        self.write_to(&mut file)?;
        file.sync_all().context("fsync CVVI temp file")?;
        sync_dir(parent).context("fsync CVVI directory")?;
        std::fs::rename(&temp_path, path)
            .with_context(|| format!("rename CVVI temp file {temp_path:?}"))?;
        sync_dir(parent).context("fsync CVVI directory post-rename")?;
        Ok(())
    }

    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<()> {
        self.validate()?;
        let header_len = self.header.header_len_bytes()?;
        let written = self.header.write_to(&mut writer)?;
        if written != header_len {
            bail!("header length mismatch: expected {header_len}, wrote {written}");
        }

        for row in &self.rows {
            row.write_to(&mut writer)?;
        }

        let rows_len = rows_size_bytes(self.header.count)?;
        let slab_offset = vector_slab_offset_bytes(header_len, self.header.count)?;
        let padding_len = slab_offset
            .checked_sub(header_len + rows_len)
            .ok_or_else(|| anyhow!("padding length underflow"))?;
        if padding_len > 0 {
            writer.write_all(&vec![0u8; padding_len])?;
        }

        self.write_vectors_to(&mut writer)?;
        Ok(())
    }

    pub fn search_top_k(
        &self,
        query_vec: &[f32],
        k: usize,
        filter: Option<&SemanticFilter>,
    ) -> Result<Vec<VectorSearchResult>> {
        if query_vec.len() != self.header.dimension as usize {
            bail!(
                "query dimension mismatch: expected {}, got {}",
                self.header.dimension,
                query_vec.len()
            );
        }
        if k == 0 {
            return Ok(Vec::new());
        }

        // P1 Opt 3: Dispatch to parallel search for large indices.
        // Skip parallelism for small indices where Rayon overhead exceeds benefit.
        if *PARALLEL_SEARCH_ENABLED && self.rows.len() >= PARALLEL_THRESHOLD {
            return self.search_top_k_parallel(query_vec, k, filter);
        }

        self.search_top_k_sequential(query_vec, k, filter)
    }

    /// Sequential search implementation (used for small indices or when parallel is disabled).
    fn search_top_k_sequential(
        &self,
        query_vec: &[f32],
        k: usize,
        filter: Option<&SemanticFilter>,
    ) -> Result<Vec<VectorSearchResult>> {
        let mut heap = BinaryHeap::with_capacity(k + 1);
        for row in &self.rows {
            if let Some(filter) = filter
                && !filter.matches(row)
            {
                continue;
            }
            let score = self.dot_product_at(row.vec_offset, query_vec)?;
            heap.push(std::cmp::Reverse(ScoredEntry {
                score,
                message_id: row.message_id,
                chunk_idx: row.chunk_idx,
            }));
            if heap.len() > k {
                heap.pop();
            }
        }

        let mut results: Vec<VectorSearchResult> = heap
            .into_iter()
            .map(|entry| VectorSearchResult {
                message_id: entry.0.message_id,
                chunk_idx: entry.0.chunk_idx,
                score: entry.0.score,
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        Ok(results)
    }

    /// P1 Opt 3: Parallel search using Rayon for large indices.
    /// Uses par_chunks with thread-local heaps, then merges results.
    fn search_top_k_parallel(
        &self,
        query_vec: &[f32],
        k: usize,
        filter: Option<&SemanticFilter>,
    ) -> Result<Vec<VectorSearchResult>> {
        // Parallel scan with thread-local heaps.
        // Each chunk maintains its own top-k heap to avoid contention.
        let partial_results: Result<Vec<Vec<ScoredEntry>>> = self
            .rows
            .par_chunks(PARALLEL_CHUNK_SIZE)
            .map(|chunk| {
                let mut local_heap = BinaryHeap::with_capacity(k + 1);
                for row in chunk {
                    if let Some(f) = filter
                        && !f.matches(row)
                    {
                        continue;
                    }
                    let score = self.dot_product_at(row.vec_offset, query_vec)?;
                    local_heap.push(std::cmp::Reverse(ScoredEntry {
                        score,
                        message_id: row.message_id,
                        chunk_idx: row.chunk_idx,
                    }));
                    if local_heap.len() > k {
                        local_heap.pop();
                    }
                }
                // Extract entries from heap (they're wrapped in Reverse).
                Ok(local_heap.into_iter().map(|r| r.0).collect())
            })
            .collect();
        let partial_results = partial_results?;

        // Merge thread-local results into final top-k.
        let mut final_heap = BinaryHeap::with_capacity(k + 1);
        for entries in partial_results {
            for entry in entries {
                final_heap.push(std::cmp::Reverse(entry));
                if final_heap.len() > k {
                    final_heap.pop();
                }
            }
        }

        let mut results: Vec<VectorSearchResult> = final_heap
            .into_iter()
            .map(|entry| VectorSearchResult {
                message_id: entry.0.message_id,
                chunk_idx: entry.0.chunk_idx,
                score: entry.0.score,
            })
            .collect();

        // Deterministic ordering: sort by score desc, then message_id for tie-breaking.
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        Ok(results)
    }

    pub fn search_top_k_collapsed(
        &self,
        query_vec: &[f32],
        k: usize,
        filter: Option<&SemanticFilter>,
    ) -> Result<Vec<VectorSearchResult>> {
        if query_vec.len() != self.header.dimension as usize {
            bail!(
                "query dimension mismatch: expected {}, got {}",
                self.header.dimension,
                query_vec.len()
            );
        }
        if k == 0 {
            return Ok(Vec::new());
        }

        let mut best_by_message: HashMap<u64, VectorSearchResult> = HashMap::new();
        for row in &self.rows {
            if let Some(filter) = filter
                && !filter.matches(row)
            {
                continue;
            }
            let score = self.dot_product_at(row.vec_offset, query_vec)?;
            best_by_message
                .entry(row.message_id)
                .and_modify(|entry| {
                    if score > entry.score {
                        entry.score = score;
                        entry.chunk_idx = row.chunk_idx;
                    }
                })
                .or_insert(VectorSearchResult {
                    message_id: row.message_id,
                    chunk_idx: row.chunk_idx,
                    score,
                });
        }

        let mut results: Vec<VectorSearchResult> = best_by_message.into_values().collect();
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        if results.len() > k {
            results.truncate(k);
        }
        Ok(results)
    }

    pub fn vector_at_f32(&self, row: &VectorRow) -> Result<Vec<f32>> {
        let dimension = self.header.dimension as usize;
        match &self.vectors {
            VectorStorage::F32(values) => {
                let start = vector_offset_to_index(row.vec_offset, 4)?;
                let end = start
                    .checked_add(dimension)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let slice = values
                    .get(start..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                Ok(slice.to_vec())
            }
            VectorStorage::F16(values) => {
                let start = vector_offset_to_index(row.vec_offset, 2)?;
                let end = start
                    .checked_add(dimension)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let slice = values
                    .get(start..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                Ok(slice.iter().map(|v| f32::from(*v)).collect())
            }
            VectorStorage::PreconvertedF32(values) => {
                // P0 Opt 1: Pre-converted from F16, vec_offset is in F16 byte terms.
                let start = vector_offset_to_index(row.vec_offset, 2)?;
                let end = start
                    .checked_add(dimension)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let slice = values
                    .get(start..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                Ok(slice.to_vec())
            }
            VectorStorage::Mmap { mmap, offset, .. } => {
                let bytes_per = self.header.quantization.bytes_per_component();
                let base = offset
                    .checked_add(
                        usize::try_from(row.vec_offset)
                            .map_err(|_| anyhow!("vector offset out of range"))?,
                    )
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let byte_len = dimension
                    .checked_mul(bytes_per)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let end = base
                    .checked_add(byte_len)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let bytes = mmap
                    .get(base..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                match self.header.quantization {
                    Quantization::F32 => {
                        let slice = bytes_as_f32(bytes)?;
                        Ok(slice.to_vec())
                    }
                    Quantization::F16 => {
                        let slice = bytes_as_f16(bytes)?;
                        Ok(slice.iter().map(|v| f32::from(*v)).collect())
                    }
                }
            }
        }
    }

    pub fn header(&self) -> &CvviHeader {
        &self.header
    }

    pub fn rows(&self) -> &[VectorRow] {
        &self.rows
    }

    fn validate(&self) -> Result<()> {
        self.header.validate()?;
        if self.rows.len() != self.header.count as usize {
            bail!(
                "row count mismatch: expected {}, got {}",
                self.header.count,
                self.rows.len()
            );
        }

        let expected_slab = vector_slab_size_bytes(
            self.header.count,
            self.header.dimension,
            self.header.quantization,
        )?;
        let actual_slab = self.vectors.len_bytes(self.header.quantization)?;
        if expected_slab != actual_slab {
            bail!(
                "vector slab size mismatch: expected {}, got {}",
                expected_slab,
                actual_slab
            );
        }

        validate_row_offsets(
            &self.rows,
            self.header.dimension as usize,
            self.header.quantization,
            expected_slab,
        )?;
        Ok(())
    }

    fn write_vectors_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        if cfg!(target_endian = "big") {
            bail!("CVVI write is only supported on little-endian targets");
        }
        match &self.vectors {
            VectorStorage::F32(values) => {
                let bytes = f32_as_bytes(values);
                writer.write_all(bytes)?;
            }
            VectorStorage::F16(values) => {
                let bytes = f16_as_bytes(values);
                writer.write_all(bytes)?;
            }
            VectorStorage::PreconvertedF32(values) => {
                // P0 Opt 1: Convert back to F16 for storage (header.quantization == F16).
                let f16_slab: Vec<f16> = values.iter().map(|v| f16::from_f32(*v)).collect();
                let bytes = f16_as_bytes(&f16_slab);
                writer.write_all(bytes)?;
            }
            VectorStorage::Mmap { mmap, offset, len } => {
                let bytes = mmap
                    .get(*offset..offset + len)
                    .ok_or_else(|| anyhow!("vector slab out of bounds"))?;
                writer.write_all(bytes)?;
            }
        }
        Ok(())
    }

    fn dot_product_at(&self, vec_offset: u64, query: &[f32]) -> Result<f32> {
        match &self.vectors {
            VectorStorage::F32(values) => {
                let start = vector_offset_to_index(vec_offset, 4)?;
                let end = start
                    .checked_add(query.len())
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let slice = values
                    .get(start..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                Ok(dot_product(slice, query))
            }
            VectorStorage::F16(values) => {
                let start = vector_offset_to_index(vec_offset, 2)?;
                let end = start
                    .checked_add(query.len())
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let slice = values
                    .get(start..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                Ok(dot_product_f16(slice, query))
            }
            VectorStorage::PreconvertedF32(values) => {
                // P0 Opt 1: Pre-converted from F16, so vec_offset is still in F16 byte terms.
                // Use 2 as divisor (F16 bytes per component) to get element index.
                let start = vector_offset_to_index(vec_offset, 2)?;
                let end = start
                    .checked_add(query.len())
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let slice = values
                    .get(start..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                Ok(dot_product(slice, query))
            }
            VectorStorage::Mmap { mmap, offset, len } => {
                let bytes_per = self.header.quantization.bytes_per_component();
                let base = offset
                    .checked_add(
                        usize::try_from(vec_offset)
                            .map_err(|_| anyhow!("vector offset out of range"))?,
                    )
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let byte_len = query
                    .len()
                    .checked_mul(bytes_per)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let end = base
                    .checked_add(byte_len)
                    .ok_or_else(|| anyhow!("vector slice overflow"))?;
                let bytes = mmap
                    .get(base..end)
                    .ok_or_else(|| anyhow!("vector slice out of bounds"))?;
                if base + byte_len > offset + len {
                    bail!("vector slice out of bounds");
                }
                match self.header.quantization {
                    Quantization::F32 => {
                        let slice = bytes_as_f32(bytes)?;
                        Ok(dot_product(slice, query))
                    }
                    Quantization::F16 => {
                        let slice = bytes_as_f16(bytes)?;
                        Ok(dot_product_f16(slice, query))
                    }
                }
            }
        }
    }
}

pub fn rows_size_bytes(count: u32) -> Result<usize> {
    (count as usize)
        .checked_mul(ROW_SIZE_BYTES)
        .ok_or_else(|| anyhow!("row size overflow for count {count}"))
}

pub fn vector_slab_offset_bytes(header_len: usize, count: u32) -> Result<usize> {
    let rows_len = rows_size_bytes(count)?;
    let end = header_len
        .checked_add(rows_len)
        .ok_or_else(|| anyhow!("offset overflow"))?;
    Ok(align_up(end, VECTOR_ALIGN_BYTES))
}

pub fn vector_slab_size_bytes(
    count: u32,
    dimension: u32,
    quantization: Quantization,
) -> Result<usize> {
    let components = (count as usize)
        .checked_mul(dimension as usize)
        .ok_or_else(|| anyhow!("vector slab size overflow"))?;
    components
        .checked_mul(quantization.bytes_per_component())
        .ok_or_else(|| anyhow!("vector slab size overflow"))
}

fn align_up(value: usize, align: usize) -> usize {
    if align == 0 {
        return value;
    }
    let rem = value % align;
    if rem == 0 {
        value
    } else {
        value + (align - rem)
    }
}

fn map_filter_set(keys: &HashSet<String>, map: &HashMap<String, u32>) -> Option<HashSet<u32>> {
    if keys.is_empty() {
        return None;
    }
    let mut set = HashSet::new();
    for key in keys {
        if let Some(id) = map.get(key) {
            set.insert(*id);
        }
    }
    Some(set)
}

fn source_id_hash(source_id: &str) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(source_id.as_bytes());
    hasher.finalize()
}

#[derive(Debug, Clone)]
struct ScoredEntry {
    score: f32,
    message_id: u64,
    chunk_idx: u8,
}

impl PartialEq for ScoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal
            && self.message_id == other.message_id
            && self.chunk_idx == other.chunk_idx
    }
}

impl Eq for ScoredEntry {}

impl PartialOrd for ScoredEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.message_id.cmp(&other.message_id))
            .then_with(|| self.chunk_idx.cmp(&other.chunk_idx))
    }
}

impl VectorStorage {
    fn len_bytes(&self, quantization: Quantization) -> Result<usize> {
        match self {
            VectorStorage::F32(values) => {
                if quantization != Quantization::F32 {
                    bail!("vector storage quantization mismatch (expected f32)");
                }
                values
                    .len()
                    .checked_mul(4)
                    .ok_or_else(|| anyhow!("vector slab size overflow"))
            }
            VectorStorage::F16(values) => {
                if quantization != Quantization::F16 {
                    bail!("vector storage quantization mismatch (expected f16)");
                }
                values
                    .len()
                    .checked_mul(2)
                    .ok_or_else(|| anyhow!("vector slab size overflow"))
            }
            VectorStorage::PreconvertedF32(values) => {
                // P0 Opt 1: Pre-converted from F16, header.quantization is F16.
                // Return the equivalent F16 byte size for validation.
                if quantization != Quantization::F16 {
                    bail!("vector storage quantization mismatch (expected f16 for preconverted)");
                }
                values
                    .len()
                    .checked_mul(2) // Each F32 element represents one F16 value
                    .ok_or_else(|| anyhow!("vector slab size overflow"))
            }
            VectorStorage::Mmap { len, .. } => Ok(*len),
        }
    }
}

fn validate_row_offsets(
    rows: &[VectorRow],
    dimension: usize,
    quantization: Quantization,
    slab_size: usize,
) -> Result<()> {
    let bytes_per = quantization.bytes_per_component();
    let vector_bytes = dimension
        .checked_mul(bytes_per)
        .ok_or_else(|| anyhow!("vector size overflow"))?;
    for (idx, row) in rows.iter().enumerate() {
        let offset = usize::try_from(row.vec_offset)
            .map_err(|_| anyhow!("row {idx} vector offset out of range"))?;
        if offset % bytes_per != 0 {
            bail!("row {idx} vector offset not aligned");
        }
        let end = offset
            .checked_add(vector_bytes)
            .ok_or_else(|| anyhow!("row {idx} vector offset overflow"))?;
        if end > slab_size {
            bail!("row {idx} vector offset out of bounds");
        }
    }
    Ok(())
}

fn vector_offset_to_index(offset: u64, bytes_per: usize) -> Result<usize> {
    if bytes_per == 0 {
        bail!("bytes_per_component must be non-zero");
    }
    let bytes_per_u64 = bytes_per as u64;
    if !offset.is_multiple_of(bytes_per_u64) {
        bail!("vector offset is not aligned to component size");
    }
    let index = offset / bytes_per_u64;
    usize::try_from(index).map_err(|_| anyhow!("vector offset out of range"))
}

fn bytes_as_f32(bytes: &[u8]) -> Result<&[f32]> {
    if !bytes.len().is_multiple_of(4) {
        bail!("f32 byte slice length is not a multiple of 4");
    }
    // SAFETY: we validate length and alignment before using the slice as f32.
    let (prefix, aligned, suffix) = unsafe { bytes.align_to::<f32>() };
    if !prefix.is_empty() || !suffix.is_empty() {
        bail!("f32 byte slice is not aligned");
    }
    Ok(aligned)
}

fn bytes_as_f16(bytes: &[u8]) -> Result<&[f16]> {
    if !bytes.len().is_multiple_of(2) {
        bail!("f16 byte slice length is not a multiple of 2");
    }
    // SAFETY: we validate length and alignment before using the slice as f16.
    let (prefix, aligned, suffix) = unsafe { bytes.align_to::<f16>() };
    if !prefix.is_empty() || !suffix.is_empty() {
        bail!("f16 byte slice is not aligned");
    }
    Ok(aligned)
}

fn f32_as_bytes(values: &[f32]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, values.len() * 4) }
}

fn f16_as_bytes(values: &[f16]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, values.len() * 2) }
}

/// Scalar dot product (fallback when SIMD is disabled).
#[inline]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// P0 Opt 2: SIMD dot product using wide crate.
/// Processes 8 floats per iteration using AVX2/SSE on x86_64 or NEON on ARM.
/// Note: SIMD reorders FP operations, causing ~1e-7 relative error vs scalar.
/// This is acceptable as it doesn't change ranking order.
#[inline]
fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;

    let chunks_a = a.chunks_exact(8);
    let chunks_b = b.chunks_exact(8);
    let remainder_a = chunks_a.remainder();
    let remainder_b = chunks_b.remainder();

    let mut sum = f32x8::ZERO;
    for (ca, cb) in chunks_a.zip(chunks_b) {
        // SAFETY: chunks_exact guarantees exactly 8 elements.
        let arr_a: [f32; 8] = ca.try_into().unwrap();
        let arr_b: [f32; 8] = cb.try_into().unwrap();
        sum += f32x8::from(arr_a) * f32x8::from(arr_b);
    }

    let mut scalar_sum: f32 = sum.reduce_add();
    for (a, b) in remainder_a.iter().zip(remainder_b) {
        scalar_sum += a * b;
    }
    scalar_sum
}

/// Bench-only wrapper for scalar dot product.
#[doc(hidden)]
pub fn dot_product_scalar_bench(a: &[f32], b: &[f32]) -> f32 {
    dot_product_scalar(a, b)
}

/// Bench-only wrapper for SIMD dot product.
#[doc(hidden)]
pub fn dot_product_simd_bench(a: &[f32], b: &[f32]) -> f32 {
    dot_product_simd(a, b)
}

/// Cached SIMD enable flag (checked once at first use).
static SIMD_DOT_ENABLED: once_cell::sync::Lazy<bool> = once_cell::sync::Lazy::new(|| {
    dotenvy::var("CASS_SIMD_DOT")
        .map(|v| v != "0" && v.to_lowercase() != "false")
        .unwrap_or(true)
});

/// Dispatches to SIMD or scalar dot product based on CASS_SIMD_DOT env var.
/// Default: SIMD enabled. Set CASS_SIMD_DOT=0 to disable.
#[inline]
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    if *SIMD_DOT_ENABLED {
        dot_product_simd(a, b)
    } else {
        dot_product_scalar(a, b)
    }
}

/// Scalar f16 dot product (fallback when SIMD is disabled).
#[inline]
fn dot_product_f16_scalar(a: &[f16], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| f32::from(*x) * y).sum()
}

/// Opt 1.1: SIMD-accelerated f16 dot product using wide crate.
/// Batches f16→f32 conversion and uses 8-wide SIMD operations.
/// Achieves 40-60% speedup over scalar implementation for typical embedding sizes.
/// Note: SIMD reorders FP operations, causing ~1e-6 relative error vs scalar.
/// This is acceptable as it doesn't change ranking order.
#[inline]
fn dot_product_f16_simd(a: &[f16], b: &[f32]) -> f32 {
    use wide::f32x8;

    let chunks = a.len() / 8;
    let mut sum = f32x8::ZERO;

    // Main SIMD loop - process 8 elements at a time
    // Batch f16→f32 conversion for better cache utilization
    for i in 0..chunks {
        let base = i * 8;
        // Convert 8 f16 values to f32 array
        // Using explicit indexing for clarity and bounds check elision
        let a_f32 = [
            f32::from(a[base]),
            f32::from(a[base + 1]),
            f32::from(a[base + 2]),
            f32::from(a[base + 3]),
            f32::from(a[base + 4]),
            f32::from(a[base + 5]),
            f32::from(a[base + 6]),
            f32::from(a[base + 7]),
        ];
        // b is already f32, just need to copy into array for SIMD
        let b_f32 = [
            b[base],
            b[base + 1],
            b[base + 2],
            b[base + 3],
            b[base + 4],
            b[base + 5],
            b[base + 6],
            b[base + 7],
        ];
        sum += f32x8::from(a_f32) * f32x8::from(b_f32);
    }

    // Reduce SIMD accumulator to scalar
    let mut scalar_sum = sum.reduce_add();

    // Handle remainder (0-7 elements)
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        scalar_sum += f32::from(a[i]) * b[i];
    }

    scalar_sum
}

/// Bench-only wrapper for scalar f16 dot product.
#[doc(hidden)]
pub fn dot_product_f16_scalar_bench(a: &[f16], b: &[f32]) -> f32 {
    dot_product_f16_scalar(a, b)
}

/// Bench-only wrapper for SIMD f16 dot product.
#[doc(hidden)]
pub fn dot_product_f16_simd_bench(a: &[f16], b: &[f32]) -> f32 {
    dot_product_f16_simd(a, b)
}

/// Dispatches to SIMD or scalar f16 dot product based on CASS_SIMD_DOT env var.
/// Default: SIMD enabled. Set CASS_SIMD_DOT=0 to disable.
#[inline]
fn dot_product_f16(a: &[f16], b: &[f32]) -> f32 {
    if *SIMD_DOT_ENABLED {
        dot_product_f16_simd(a, b)
    } else {
        dot_product_f16_scalar(a, b)
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()?;
    Ok(())
}

fn read_u8<R: Read>(reader: &mut R, header_bytes: &mut Vec<u8>) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    header_bytes.extend_from_slice(&buf);
    Ok(buf[0])
}

fn read_u16_le<R: Read>(reader: &mut R, header_bytes: &mut Vec<u8>) -> Result<u16> {
    let buf = read_exact_array::<2, _>(reader, header_bytes)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32_le<R: Read>(reader: &mut R, header_bytes: &mut Vec<u8>) -> Result<u32> {
    let buf = read_exact_array::<4, _>(reader, header_bytes)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u32_le_no_accum<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_exact_vec<R: Read>(
    reader: &mut R,
    len: usize,
    header_bytes: &mut Vec<u8>,
) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    header_bytes.extend_from_slice(&buf);
    Ok(buf)
}

fn read_exact_array<const N: usize, R: Read>(
    reader: &mut R,
    header_bytes: &mut Vec<u8>,
) -> Result<[u8; N]> {
    let mut buf = [0u8; N];
    reader.read_exact(&mut buf)?;
    header_bytes.extend_from_slice(&buf);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::tempdir;

    fn assert_send<T: Send>() {}

    fn assert_sync<T: Sync>() {}

    struct TinyRng(u32);

    impl TinyRng {
        fn new(seed: u32) -> Self {
            Self(seed)
        }

        fn next_u32(&mut self) -> u32 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            self.0 = x;
            x
        }

        fn next_f32(&mut self) -> f32 {
            let unit = self.next_u32() as f32 / u32::MAX as f32;
            unit.mul_add(2.0, -1.0)
        }
    }

    fn sample_entries() -> Vec<VectorEntry> {
        vec![
            VectorEntry {
                message_id: 1,
                created_at_ms: 1000,
                agent_id: 1,
                workspace_id: 10,
                source_id: 100,
                role: 0,
                chunk_idx: 0,
                content_hash: [0x11; 32],
                vector: vec![1.0, 0.0, 0.0],
            },
            VectorEntry {
                message_id: 2,
                created_at_ms: 2000,
                agent_id: 1,
                workspace_id: 10,
                source_id: 100,
                role: 1,
                chunk_idx: 0,
                content_hash: [0x22; 32],
                vector: vec![0.0, 1.0, 0.0],
            },
            VectorEntry {
                message_id: 3,
                created_at_ms: 3000,
                agent_id: 2,
                workspace_id: 10,
                source_id: 100,
                role: 1,
                chunk_idx: 0,
                content_hash: [0x33; 32],
                vector: vec![0.0, 0.0, 1.0],
            },
        ]
    }

    #[test]
    fn vector_row_is_send_sync() {
        assert_send::<VectorRow>();
        assert_sync::<VectorRow>();
    }

    #[test]
    fn vector_index_is_sync() {
        assert_send::<&VectorIndex>();
        assert_sync::<VectorIndex>();
    }

    #[test]
    fn header_roundtrip_and_crc() -> Result<()> {
        let header = CvviHeader::new("minilm-384", "e4ce9877", 384, Quantization::F16, 42)?;
        let mut bytes = Vec::new();
        header.write_to(&mut bytes)?;

        let parsed = CvviHeader::read_from(bytes.as_slice())?;
        assert_eq!(parsed, header);
        Ok(())
    }

    #[test]
    fn header_crc_detects_corruption() -> Result<()> {
        let header = CvviHeader::new("hash-256", "rev", 256, Quantization::F32, 1)?;
        let mut bytes = Vec::new();
        header.write_to(&mut bytes)?;

        // Flip a byte in the embedder id to break CRC.
        let mut corrupted = bytes.clone();
        if corrupted.len() > 8 {
            corrupted[8] ^= 0b0001_0000;
        }

        let result = CvviHeader::read_from(corrupted.as_slice());
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn row_roundtrip() -> Result<()> {
        let row = VectorRow {
            message_id: 7,
            created_at_ms: 1234,
            agent_id: 2,
            workspace_id: 3,
            source_id: 4,
            role: 1,
            chunk_idx: 0,
            vec_offset: 128,
            content_hash: [0xAB; 32],
        };

        let bytes = row.to_bytes();
        let parsed = VectorRow::from_bytes(&bytes)?;
        assert_eq!(parsed, row);
        Ok(())
    }

    #[test]
    fn vector_slab_offset_is_aligned() -> Result<()> {
        let header = CvviHeader::new("id", "rev", 128, Quantization::F16, 3)?;
        let header_len = header.header_len_bytes()?;
        let offset = vector_slab_offset_bytes(header_len, header.count)?;
        assert_eq!(offset % VECTOR_ALIGN_BYTES, 0);
        Ok(())
    }

    #[test]
    fn index_roundtrip_save_load() -> Result<()> {
        let entries = sample_entries();
        let index = VectorIndex::build("hash-3", "rev", 3, Quantization::F32, entries)?;
        let dir = tempdir()?;
        let path = dir.path().join("index.cvvi");
        index.save(&path)?;

        let loaded = VectorIndex::load(&path)?;
        assert_eq!(loaded.header(), index.header());
        assert_eq!(loaded.rows(), index.rows());
        for row in loaded.rows() {
            let original = index.vector_at_f32(row)?;
            let roundtrip = loaded.vector_at_f32(row)?;
            assert_eq!(original, roundtrip);
        }
        Ok(())
    }

    #[test]
    fn search_respects_filter() -> Result<()> {
        let entries = sample_entries();
        let index = VectorIndex::build("hash-3", "rev", 3, Quantization::F32, entries)?;
        let filter = SemanticFilter {
            agents: Some(HashSet::from([2])),
            ..Default::default()
        };
        let results = index.search_top_k(&[0.0, 0.0, 1.0], 5, Some(&filter))?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message_id, 3);
        Ok(())
    }

    #[test]
    fn f16_and_f32_rankings_match() -> Result<()> {
        let entries = sample_entries();
        let index_f32 = VectorIndex::build("hash-3", "rev", 3, Quantization::F32, entries.clone())?;
        let index_f16 = VectorIndex::build("hash-3", "rev", 3, Quantization::F16, entries)?;
        let query = [0.9, 0.1, -0.2];
        let results_f32 = index_f32.search_top_k(&query, 3, None)?;
        let results_f16 = index_f16.search_top_k(&query, 3, None)?;
        let ids_f32: Vec<u64> = results_f32.iter().map(|r| r.message_id).collect();
        let ids_f16: Vec<u64> = results_f16.iter().map(|r| r.message_id).collect();
        assert_eq!(ids_f16, ids_f32);
        Ok(())
    }

    #[test]
    fn semantic_filter_from_search_filters_maps_ids() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            r#"
            CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
            CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
            CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT NOT NULL);
            INSERT INTO agents (id, slug) VALUES (1, 'codex'), (2, 'claude');
            INSERT INTO workspaces (id, path) VALUES (10, '/ws/alpha');
            INSERT INTO sources (id, kind) VALUES ('local', 'local'), ('laptop', 'ssh');
            "#,
        )?;

        let maps = SemanticFilterMaps::from_connection(&conn)?;
        let mut filters = SearchFilters::default();
        filters.agents.insert("codex".to_string());
        filters.workspaces.insert("/ws/alpha".to_string());
        filters.source_filter = SourceFilter::Remote;

        let semantic = SemanticFilter::from_search_filters(&filters, &maps)?;
        assert_eq!(semantic.agents, Some(HashSet::from([1])));
        assert_eq!(semantic.workspaces, Some(HashSet::from([10])));
        assert_eq!(
            semantic.sources,
            Some(HashSet::from([maps.source_id("laptop")]))
        );
        Ok(())
    }

    #[test]
    fn role_code_parsing_accepts_known_roles() -> Result<()> {
        let roles = parse_role_codes(["user", "assistant", "system", "tool"])?;
        assert!(roles.contains(&ROLE_USER));
        assert!(roles.contains(&ROLE_ASSISTANT));
        assert!(roles.contains(&ROLE_SYSTEM));
        assert!(roles.contains(&ROLE_TOOL));
        Ok(())
    }

    #[test]
    fn role_code_parsing_rejects_unknown_roles() {
        let err = parse_role_codes(["unknown"]);
        assert!(err.is_err());
    }

    #[test]
    fn search_respects_role_filter() -> Result<()> {
        // Sample entries have: msg1=role 0 (user), msg2=role 1 (assistant), msg3=role 1 (assistant)
        let entries = sample_entries();
        let index = VectorIndex::build("hash-3", "rev", 3, Quantization::F32, entries)?;

        // Filter to user role only (role 0)
        let filter = SemanticFilter {
            roles: Some(HashSet::from([ROLE_USER])),
            ..Default::default()
        };
        let results = index.search_top_k(&[1.0, 0.0, 0.0], 5, Some(&filter))?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message_id, 1); // Only message with role 0

        // Filter to assistant role only (role 1)
        let filter = SemanticFilter {
            roles: Some(HashSet::from([ROLE_ASSISTANT])),
            ..Default::default()
        };
        let results = index.search_top_k(&[0.0, 1.0, 0.0], 5, Some(&filter))?;
        assert_eq!(results.len(), 2); // Messages 2 and 3 have role 1

        // Filter to both roles - should get all 3
        let filter = SemanticFilter {
            roles: Some(HashSet::from([ROLE_USER, ROLE_ASSISTANT])),
            ..Default::default()
        };
        let results = index.search_top_k(&[0.5, 0.5, 0.0], 5, Some(&filter))?;
        assert_eq!(results.len(), 3);

        Ok(())
    }

    #[test]
    fn f16_preconvert_load_produces_same_search_results() -> Result<()> {
        // P0 Opt 1: Test that F16 pre-conversion at load time produces identical results.
        // Build an F16 index, save it, load it (with pre-conversion), and verify search results match.
        let entries = sample_entries();
        let index = VectorIndex::build("hash-3", "rev", 3, Quantization::F16, entries.clone())?;
        let dir = tempdir()?;
        let path = dir.path().join("index.cvvi");
        index.save(&path)?;

        // Load the index (default: F16 pre-conversion enabled).
        let loaded = VectorIndex::load(&path)?;

        // Verify header is preserved.
        assert_eq!(loaded.header().quantization, Quantization::F16);
        assert_eq!(loaded.header().count, index.header().count);
        assert_eq!(loaded.header().dimension, index.header().dimension);

        // Verify search produces same ranking as original.
        let query = [0.9, 0.1, -0.2];
        let original_results = index.search_top_k(&query, 3, None)?;
        let loaded_results = loaded.search_top_k(&query, 3, None)?;

        let ids_orig: Vec<u64> = original_results.iter().map(|r| r.message_id).collect();
        let ids_loaded: Vec<u64> = loaded_results.iter().map(|r| r.message_id).collect();
        assert_eq!(
            ids_loaded, ids_orig,
            "Pre-converted index must return same message IDs"
        );

        // Verify scores are approximately equal (F16→F32 conversion may introduce tiny FP differences).
        for (orig, loaded) in original_results.iter().zip(loaded_results.iter()) {
            let rel_err = (orig.score - loaded.score).abs() / orig.score.abs().max(1e-10);
            assert!(
                rel_err < 1e-3,
                "Score difference too large: orig={}, loaded={}, rel_err={}",
                orig.score,
                loaded.score,
                rel_err
            );
        }

        // Verify save roundtrip preserves the index (convert back to F16).
        let path2 = dir.path().join("index2.cvvi");
        loaded.save(&path2)?;
        let reloaded = VectorIndex::load(&path2)?;
        let reloaded_results = reloaded.search_top_k(&query, 3, None)?;
        let ids_reloaded: Vec<u64> = reloaded_results.iter().map(|r| r.message_id).collect();
        assert_eq!(
            ids_reloaded, ids_orig,
            "Re-saved index must return same message IDs"
        );

        Ok(())
    }

    #[test]
    fn simd_dot_product_matches_scalar_within_tolerance() {
        // P0 Opt 2: Verify SIMD dot product matches scalar within acceptable FP tolerance.
        // SIMD reorders operations, causing ~1e-7 relative error, which is acceptable.

        // Test with various vector sizes (including those that aren't multiples of 8).
        let test_sizes = [3, 8, 16, 100, 384, 385, 1000];

        for size in test_sizes {
            let a: Vec<f32> = (0..size).map(|i| (i as f32) * 0.001).collect();
            let b: Vec<f32> = (0..size).map(|i| ((size - i) as f32) * 0.001).collect();

            let scalar = dot_product_scalar(&a, &b);
            let simd = dot_product_simd(&a, &b);

            let abs_err = (scalar - simd).abs();
            let rel_err = abs_err / scalar.abs().max(1e-10);

            assert!(
                rel_err < 1e-5,
                "SIMD dot product differs too much from scalar for size {}: scalar={}, simd={}, rel_err={}",
                size,
                scalar,
                simd,
                rel_err
            );
        }
    }

    #[test]
    fn simd_dot_product_handles_edge_cases() {
        // Test empty vectors.
        let empty: Vec<f32> = vec![];
        assert_eq!(dot_product_simd(&empty, &empty), 0.0);

        // Test single element.
        let single = vec![2.0];
        assert!((dot_product_simd(&single, &single) - 4.0).abs() < 1e-6);

        // Test 7 elements (less than one SIMD chunk).
        let seven: Vec<f32> = vec![1.0; 7];
        assert!((dot_product_simd(&seven, &seven) - 7.0).abs() < 1e-6);

        // Test exactly 8 elements (one SIMD chunk).
        let eight: Vec<f32> = vec![1.0; 8];
        assert!((dot_product_simd(&eight, &eight) - 8.0).abs() < 1e-6);

        // Test 9 elements (one SIMD chunk + remainder).
        let nine: Vec<f32> = vec![1.0; 9];
        assert!((dot_product_simd(&nine, &nine) - 9.0).abs() < 1e-6);
    }

    #[test]
    fn simd_dot_product_random_inputs() {
        let mut rng = TinyRng::new(0xC0FFEE);
        let size = 384;

        for _ in 0..1000 {
            let a: Vec<f32> = (0..size).map(|_| rng.next_f32()).collect();
            let b: Vec<f32> = (0..size).map(|_| rng.next_f32()).collect();

            let scalar = dot_product_scalar(&a, &b);
            let simd = dot_product_simd(&a, &b);

            let rel_err = (scalar - simd).abs() / scalar.abs().max(1e-10);
            // Use 2e-4 tolerance for SIMD - FP operation reordering can cause small differences.
            // This matches the f16 tolerance and is acceptable as it doesn't affect ranking.
            assert!(
                rel_err < 2e-4,
                "Random SIMD dot product mismatch: scalar={}, simd={}, rel_err={}",
                scalar,
                simd,
                rel_err
            );
        }
    }

    #[test]
    fn simd_dot_product_large_values() {
        let a = vec![1e10_f32; 384];
        let b = vec![1e-10_f32; 384];
        let simd = dot_product_simd(&a, &b);
        assert!(
            (simd - 384.0).abs() < 1e-2,
            "Large value dot product drifted: {}",
            simd
        );
    }

    #[test]
    fn simd_dot_product_preserves_rank_order_for_separated_scores() {
        let query = vec![0.75_f32, -1.5_f32, 0.25_f32, 2.0_f32];
        let mut items: Vec<(u64, Vec<f32>)> = (1..=16)
            .map(|i| {
                let scale = i as f32 * 0.5;
                let vec: Vec<f32> = query.iter().map(|v| v * scale).collect();
                (i as u64, vec)
            })
            .collect();
        items.push((99, vec![5.0, -4.0, 3.0, -2.0]));

        let mut scalar_scores: Vec<(u64, f32)> = items
            .iter()
            .map(|(id, v)| (*id, dot_product_scalar(v, &query)))
            .collect();
        let mut simd_scores: Vec<(u64, f32)> = items
            .iter()
            .map(|(id, v)| (*id, dot_product_simd(v, &query)))
            .collect();

        let rank = |scores: &mut Vec<(u64, f32)>| {
            scores.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            scores.iter().map(|(id, _)| *id).collect::<Vec<_>>()
        };

        let scalar_rank = rank(&mut scalar_scores);
        let simd_rank = rank(&mut simd_scores);
        assert_eq!(
            scalar_rank, simd_rank,
            "SIMD ranking changed for separated scores"
        );
    }

    #[test]
    fn parallel_search_matches_sequential() -> Result<()> {
        // P1 Opt 3: Verify parallel search produces same results as sequential.
        // Create an index large enough to trigger parallel search.
        let dimension = 64;
        let count = PARALLEL_THRESHOLD + 1000; // Ensure parallel threshold is exceeded.

        let entries: Vec<VectorEntry> = (0..count)
            .map(|i| {
                // Create vectors with varying values so search produces distinct scores.
                let vector: Vec<f32> = (0..dimension)
                    .map(|d| ((i + d * 7) % 100) as f32 / 100.0)
                    .collect();
                VectorEntry {
                    message_id: i as u64,
                    created_at_ms: i as i64 * 1000,
                    agent_id: (i % 4) as u32,
                    workspace_id: 1,
                    source_id: 1,
                    role: (i % 2) as u8,
                    chunk_idx: 0,
                    content_hash: [0u8; 32],
                    vector,
                }
            })
            .collect();

        let index = VectorIndex::build("test", "rev", dimension, Quantization::F32, entries)?;

        // Verify the index is large enough for parallel search.
        assert!(
            index.rows().len() >= PARALLEL_THRESHOLD,
            "Index should exceed parallel threshold"
        );

        // Create a query vector.
        let query: Vec<f32> = (0..dimension).map(|d| (d % 10) as f32 / 10.0).collect();

        // Get results from sequential search.
        let sequential_results = index.search_top_k_sequential(&query, 25, None)?;

        // Get results from parallel search.
        let parallel_results = index.search_top_k_parallel(&query, 25, None)?;

        // Verify same message IDs in same order.
        let seq_ids: Vec<u64> = sequential_results.iter().map(|r| r.message_id).collect();
        let par_ids: Vec<u64> = parallel_results.iter().map(|r| r.message_id).collect();
        assert_eq!(
            seq_ids, par_ids,
            "Parallel search must return same message IDs as sequential"
        );

        // Verify scores are identical (both use same dot product function).
        for (seq, par) in sequential_results.iter().zip(parallel_results.iter()) {
            assert!(
                (seq.score - par.score).abs() < 1e-6,
                "Score mismatch: seq={}, par={}",
                seq.score,
                par.score
            );
        }

        Ok(())
    }

    #[test]
    fn parallel_search_respects_filter() -> Result<()> {
        // P1 Opt 3: Verify parallel search respects filters correctly.
        let dimension = 32;
        let count = PARALLEL_THRESHOLD + 500;

        let entries: Vec<VectorEntry> = (0..count)
            .map(|i| {
                let vector: Vec<f32> = (0..dimension)
                    .map(|d| ((i + d * 3) % 50) as f32 / 50.0)
                    .collect();
                VectorEntry {
                    message_id: i as u64,
                    created_at_ms: i as i64 * 1000,
                    agent_id: (i % 4) as u32, // Agents 0, 1, 2, 3
                    workspace_id: 1,
                    source_id: 1,
                    role: 0,
                    chunk_idx: 0,
                    content_hash: [0u8; 32],
                    vector,
                }
            })
            .collect();

        let index = VectorIndex::build("test", "rev", dimension, Quantization::F32, entries)?;

        let query: Vec<f32> = (0..dimension)
            .map(|d| d as f32 / dimension as f32)
            .collect();

        // Filter to agent 0 only.
        let filter = SemanticFilter {
            agents: Some(HashSet::from([0u32])),
            ..Default::default()
        };

        let sequential_results = index.search_top_k_sequential(&query, 10, Some(&filter))?;
        let parallel_results = index.search_top_k_parallel(&query, 10, Some(&filter))?;

        // Verify all results have agent_id 0.
        for result in &parallel_results {
            let row = index
                .rows()
                .iter()
                .find(|r| r.message_id == result.message_id);
            assert!(
                row.map(|r| r.agent_id) == Some(0),
                "Parallel search returned wrong agent_id for message {}",
                result.message_id
            );
        }

        // Verify same results as sequential.
        let seq_ids: Vec<u64> = sequential_results.iter().map(|r| r.message_id).collect();
        let par_ids: Vec<u64> = parallel_results.iter().map(|r| r.message_id).collect();
        assert_eq!(
            seq_ids, par_ids,
            "Filtered parallel search must match sequential"
        );

        Ok(())
    }

    #[test]
    fn parallel_search_deterministic() -> Result<()> {
        // P1 Opt 3: Parallel results should be deterministic across runs.
        let dimension = 48;
        let count = PARALLEL_THRESHOLD + 250;

        let entries: Vec<VectorEntry> = (0..count)
            .map(|i| {
                let vector: Vec<f32> = (0..dimension)
                    .map(|d| ((i * 31 + d * 17) % 100) as f32 / 100.0)
                    .collect();
                VectorEntry {
                    message_id: i as u64,
                    created_at_ms: i as i64 * 1000,
                    agent_id: (i % 4) as u32,
                    workspace_id: 1,
                    source_id: 1,
                    role: (i % 2) as u8,
                    chunk_idx: 0,
                    content_hash: [0u8; 32],
                    vector,
                }
            })
            .collect();

        let index = VectorIndex::build("test", "rev", dimension, Quantization::F32, entries)?;
        let mut rng = TinyRng::new(42);
        let query: Vec<f32> = (0..dimension).map(|_| rng.next_f32()).collect();

        let baseline = index.search_top_k_parallel(&query, 20, None)?;
        let baseline_ids: Vec<u64> = baseline.iter().map(|r| r.message_id).collect();

        for _ in 0..5 {
            let run = index.search_top_k_parallel(&query, 20, None)?;
            let run_ids: Vec<u64> = run.iter().map(|r| r.message_id).collect();
            assert_eq!(baseline_ids, run_ids, "Parallel search not deterministic");
        }

        Ok(())
    }

    #[test]
    fn parallel_search_multiple_queries_match() -> Result<()> {
        // P1 Opt 3: Multiple random queries should match sequential results.
        let dimension = 40;
        let count = PARALLEL_THRESHOLD + 100;

        let entries: Vec<VectorEntry> = (0..count)
            .map(|i| {
                let vector: Vec<f32> = (0..dimension)
                    .map(|d| ((i * 13 + d * 9) % 100) as f32 / 100.0)
                    .collect();
                VectorEntry {
                    message_id: i as u64,
                    created_at_ms: i as i64 * 1000,
                    agent_id: (i % 3) as u32,
                    workspace_id: 1,
                    source_id: 1,
                    role: 0,
                    chunk_idx: 0,
                    content_hash: [0u8; 32],
                    vector,
                }
            })
            .collect();

        let index = VectorIndex::build("test", "rev", dimension, Quantization::F32, entries)?;
        let mut rng = TinyRng::new(7);

        for _ in 0..10 {
            let query: Vec<f32> = (0..dimension).map(|_| rng.next_f32()).collect();
            let sequential = index.search_top_k_sequential(&query, 15, None)?;
            let parallel = index.search_top_k_parallel(&query, 15, None)?;
            let seq_ids: Vec<u64> = sequential.iter().map(|r| r.message_id).collect();
            let par_ids: Vec<u64> = parallel.iter().map(|r| r.message_id).collect();
            assert_eq!(
                seq_ids, par_ids,
                "Parallel results diverged from sequential"
            );
        }

        Ok(())
    }

    #[test]
    fn small_index_search_matches_sequential() -> Result<()> {
        // P1 Opt 3: Small indices should still match sequential results.
        let dimension = 16;
        let count = 100;

        let entries: Vec<VectorEntry> = (0..count)
            .map(|i| {
                let vector: Vec<f32> = (0..dimension)
                    .map(|d| ((i + d * 5) % 50) as f32 / 50.0)
                    .collect();
                VectorEntry {
                    message_id: i as u64,
                    created_at_ms: i as i64 * 1000,
                    agent_id: 0,
                    workspace_id: 1,
                    source_id: 1,
                    role: 0,
                    chunk_idx: 0,
                    content_hash: [0u8; 32],
                    vector,
                }
            })
            .collect();

        let index = VectorIndex::build("test", "rev", dimension, Quantization::F32, entries)?;
        let query: Vec<f32> = (0..dimension).map(|d| d as f32 / 10.0).collect();

        let expected = index.search_top_k_sequential(&query, 10, None)?;
        let actual = index.search_top_k(&query, 10, None)?;
        let expected_ids: Vec<u64> = expected.iter().map(|r| r.message_id).collect();
        let actual_ids: Vec<u64> = actual.iter().map(|r| r.message_id).collect();
        assert_eq!(expected_ids, actual_ids, "Small index results changed");

        Ok(())
    }

    // ========================================================================
    // Opt 1.1: F16 SIMD Dot Product Tests
    // ========================================================================

    #[test]
    fn f16_simd_dot_product_matches_scalar_within_tolerance() {
        // Opt 1.1: Verify f16 SIMD dot product matches scalar within acceptable FP tolerance.
        // SIMD reorders operations, causing ~1e-6 relative error, which is acceptable.

        // Test with various vector sizes (including those that aren't multiples of 8).
        let test_sizes = [3, 7, 8, 9, 15, 16, 17, 100, 384, 385, 512, 768, 1000];

        for size in test_sizes {
            let a_f16: Vec<f16> = (0..size)
                .map(|i| f16::from_f32((i as f32) * 0.001))
                .collect();
            let b_f32: Vec<f32> = (0..size).map(|i| ((size - i) as f32) * 0.001).collect();

            let scalar = dot_product_f16_scalar(&a_f16, &b_f32);
            let simd = dot_product_f16_simd(&a_f16, &b_f32);

            let abs_err = (scalar - simd).abs();
            let rel_err = abs_err / scalar.abs().max(1e-10);

            assert!(
                rel_err < 1e-4,
                "F16 SIMD dot product mismatch at size {}: scalar={}, simd={}, rel_err={}",
                size,
                scalar,
                simd,
                rel_err
            );
        }
    }

    #[test]
    fn f16_simd_dot_product_handles_edge_cases() {
        // Test empty vectors.
        let empty_f16: Vec<f16> = vec![];
        let empty_f32: Vec<f32> = vec![];
        assert_eq!(dot_product_f16_simd(&empty_f16, &empty_f32), 0.0);

        // Test single element.
        let single_f16 = vec![f16::from_f32(2.0)];
        let single_f32 = vec![3.0_f32];
        let result = dot_product_f16_simd(&single_f16, &single_f32);
        assert!(
            (result - 6.0).abs() < 1e-3,
            "Single element f16 dot product: expected ~6.0, got {}",
            result
        );

        // Test 7 elements (less than one SIMD chunk).
        let seven_f16: Vec<f16> = (0..7).map(|_| f16::from_f32(1.0)).collect();
        let seven_f32: Vec<f32> = vec![1.0; 7];
        let result = dot_product_f16_simd(&seven_f16, &seven_f32);
        assert!(
            (result - 7.0).abs() < 1e-3,
            "7 elements f16 dot product: expected ~7.0, got {}",
            result
        );

        // Test exactly 8 elements (one SIMD chunk).
        let eight_f16: Vec<f16> = (0..8).map(|_| f16::from_f32(1.0)).collect();
        let eight_f32: Vec<f32> = vec![1.0; 8];
        let result = dot_product_f16_simd(&eight_f16, &eight_f32);
        assert!(
            (result - 8.0).abs() < 1e-3,
            "8 elements f16 dot product: expected ~8.0, got {}",
            result
        );

        // Test 9 elements (one SIMD chunk + remainder).
        let nine_f16: Vec<f16> = (0..9).map(|_| f16::from_f32(1.0)).collect();
        let nine_f32: Vec<f32> = vec![1.0; 9];
        let result = dot_product_f16_simd(&nine_f16, &nine_f32);
        assert!(
            (result - 9.0).abs() < 1e-3,
            "9 elements f16 dot product: expected ~9.0, got {}",
            result
        );
    }

    #[test]
    fn f16_simd_dot_product_random_inputs() {
        let mut rng = TinyRng::new(0xF16BEEF);
        let size = 384; // Typical embedding dimension

        for _ in 0..1000 {
            let a_f16: Vec<f16> = (0..size).map(|_| f16::from_f32(rng.next_f32())).collect();
            let b_f32: Vec<f32> = (0..size).map(|_| rng.next_f32()).collect();

            let scalar = dot_product_f16_scalar(&a_f16, &b_f32);
            let simd = dot_product_f16_simd(&a_f16, &b_f32);

            // Use 2e-4 tolerance for f16 - slightly higher than f32 due to f16 precision loss
            // combined with SIMD FP reordering. This is acceptable as it doesn't affect ranking.
            let rel_err = (scalar - simd).abs() / scalar.abs().max(1e-10);
            assert!(
                rel_err < 2e-4,
                "Random f16 SIMD dot product mismatch: scalar={}, simd={}, rel_err={}",
                scalar,
                simd,
                rel_err
            );
        }
    }

    #[test]
    fn f16_simd_dot_product_real_embedding_dimensions() {
        // Test actual embedding sizes used in practice.
        let dims = [128, 256, 384, 512, 768, 1024, 1536];
        let mut rng = TinyRng::new(0xDEADBEEF);

        for dim in dims {
            let a_f16: Vec<f16> = (0..dim)
                .map(|i| f16::from_f32((i as f32).sin() * 0.5))
                .collect();
            let b_f32: Vec<f32> = (0..dim).map(|i| (i as f32).cos() * 0.5).collect();

            let scalar = dot_product_f16_scalar(&a_f16, &b_f32);
            let simd = dot_product_f16_simd(&a_f16, &b_f32);

            let rel_err = (scalar - simd).abs() / scalar.abs().max(1e-10);
            assert!(
                rel_err < 1e-4,
                "F16 SIMD dot product mismatch at dim={}: scalar={}, simd={}, rel_err={}",
                dim,
                scalar,
                simd,
                rel_err
            );

            // Also test with random values at this dimension.
            let a_f16_rand: Vec<f16> = (0..dim).map(|_| f16::from_f32(rng.next_f32())).collect();
            let b_f32_rand: Vec<f32> = (0..dim).map(|_| rng.next_f32()).collect();
            let scalar_rand = dot_product_f16_scalar(&a_f16_rand, &b_f32_rand);
            let simd_rand = dot_product_f16_simd(&a_f16_rand, &b_f32_rand);
            let rel_err_rand = (scalar_rand - simd_rand).abs() / scalar_rand.abs().max(1e-10);
            assert!(
                rel_err_rand < 1e-4,
                "F16 SIMD random at dim={}: rel_err={}",
                dim,
                rel_err_rand
            );
        }
    }

    #[test]
    fn f16_simd_dot_product_preserves_rank_order() {
        // Opt 1.1: Verify SIMD preserves ranking order for search results.
        let query_f32: Vec<f32> = vec![0.75, -1.5, 0.25, 2.0, 1.0, -0.5, 0.8, -0.2];
        let mut items: Vec<(u64, Vec<f16>)> = (1..=20)
            .map(|i| {
                let scale = i as f32 * 0.5;
                let vec: Vec<f16> = query_f32.iter().map(|v| f16::from_f32(v * scale)).collect();
                (i as u64, vec)
            })
            .collect();
        items.push((
            99,
            vec![
                f16::from_f32(5.0),
                f16::from_f32(-4.0),
                f16::from_f32(3.0),
                f16::from_f32(-2.0),
                f16::from_f32(1.5),
                f16::from_f32(-1.0),
                f16::from_f32(2.0),
                f16::from_f32(-0.5),
            ],
        ));

        let mut scalar_scores: Vec<(u64, f32)> = items
            .iter()
            .map(|(id, v)| (*id, dot_product_f16_scalar(v, &query_f32)))
            .collect();
        let mut simd_scores: Vec<(u64, f32)> = items
            .iter()
            .map(|(id, v)| (*id, dot_product_f16_simd(v, &query_f32)))
            .collect();

        let rank = |scores: &mut Vec<(u64, f32)>| {
            scores.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            scores.iter().map(|(id, _)| *id).collect::<Vec<_>>()
        };

        let scalar_rank = rank(&mut scalar_scores);
        let simd_rank = rank(&mut simd_scores);
        assert_eq!(
            scalar_rank, simd_rank,
            "F16 SIMD ranking changed for separated scores"
        );
    }
}
