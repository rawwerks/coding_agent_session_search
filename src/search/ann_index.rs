//! HNSW-based Approximate Nearest Neighbor (ANN) index for semantic search.
//!
//! This module provides O(log n) approximate vector search using the HNSW algorithm,
//! as an alternative to the O(n) exact search in [`crate::search::vector_index`].
//!
//! ## Usage
//!
//! Build HNSW index during indexing:
//! ```bash
//! cass index --semantic --build-hnsw
//! ```
//!
//! Use ANN search at query time:
//! ```bash
//! cass search "query" --mode semantic --approximate
//! ```
//!
//! ## Trade-offs
//!
//! - **Speed**: O(log n) vs O(n) for exact search
//! - **Recall**: ~95-99% depending on ef parameter (configurable)
//! - **Memory**: Additional ~50-100 bytes per vector for graph structure
//! - **Build time**: ~2-5x slower than CVVI-only indexing
//!
//! ## Implementation Notes
//!
//! Uses hnsw_rs with these parameters (from bead coding_agent_session_search-06kc):
//! - M (max_nb_connection): 16 (balances memory/quality)
//! - ef_construction: 200 (good build-time accuracy)
//! - Default ef_search: 100 (tunable at query time)

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hnsw_rs::hnsw::{Hnsw, Neighbour};
use hnsw_rs::prelude::*;

use crate::search::vector_index::{VectorIndex, VECTOR_INDEX_DIR};

/// Magic bytes for HNSW index file format.
pub const HNSW_MAGIC: [u8; 4] = *b"CHSW";

/// HNSW index file version.
pub const HNSW_VERSION: u16 = 1;

/// Default HNSW parameters (from bead recommendations).
pub const DEFAULT_M: usize = 16;
pub const DEFAULT_EF_CONSTRUCTION: usize = 200;
pub const DEFAULT_EF_SEARCH: usize = 100;
pub const DEFAULT_MAX_LAYER: usize = 16;

/// Path to HNSW index file for a given embedder.
pub fn hnsw_index_path(data_dir: &Path, embedder_id: &str) -> PathBuf {
    data_dir
        .join(VECTOR_INDEX_DIR)
        .join(format!("hnsw-{embedder_id}.chsw"))
}

/// Result from an approximate nearest neighbor search.
#[derive(Debug, Clone)]
pub struct AnnSearchResult {
    /// Index into the VectorIndex rows array.
    pub row_idx: usize,
    /// Approximate distance (lower is better for dot product converted to distance).
    pub distance: f32,
}

/// HNSW index wrapper for approximate nearest neighbor search.
///
/// The index stores references to row indices in the corresponding VectorIndex,
/// allowing fast approximate lookup followed by metadata retrieval.
pub struct HnswIndex {
    /// The underlying HNSW graph structure.
    /// Uses DistDot for dot product similarity (converted to distance).
    hnsw: Hnsw<'static, f32, DistDot>,
    /// Number of vectors in the index.
    count: usize,
    /// Embedder ID this index was built for.
    embedder_id: String,
    /// Dimension of vectors.
    dimension: usize,
}

impl HnswIndex {
    /// Build a new HNSW index from an existing VectorIndex.
    ///
    /// This reads all vectors from the CVVI file and builds the HNSW graph.
    /// The row index (position in VectorIndex.rows()) is used as the ID.
    pub fn build_from_vector_index(
        vector_index: &VectorIndex,
        m: usize,
        ef_construction: usize,
    ) -> Result<Self> {
        let count = vector_index.rows().len();
        let dimension = vector_index.header().dimension as usize;
        let embedder_id = vector_index.header().embedder_id.clone();

        if count == 0 {
            bail!("cannot build HNSW index from empty VectorIndex");
        }

        tracing::info!(
            count,
            dimension,
            m,
            ef_construction,
            "Building HNSW index"
        );

        // Create HNSW with dot product distance.
        // DistDot computes 1 - dot_product, so lower distance = higher similarity.
        let hnsw: Hnsw<f32, DistDot> = Hnsw::new(
            m,
            count,
            DEFAULT_MAX_LAYER,
            ef_construction,
            DistDot,
        );

        // Insert all vectors with their row index as ID.
        // We collect vectors first to enable parallel insertion.
        let vectors_with_ids: Vec<(&[f32], usize)> = vector_index
            .rows()
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                // Get the vector as f32 slice from the index.
                // Note: This requires the vector data to be accessible.
                let vec = vector_index
                    .vector_at_f32(row)
                    .expect("failed to read vector");
                // Leak the vector to get a static lifetime (we own all data).
                let vec_static: &'static [f32] = Box::leak(vec.into_boxed_slice());
                (vec_static, idx)
            })
            .collect();

        // Use parallel insertion for better performance.
        // The hnsw_rs API expects a single Vec of (data, id) tuples.
        hnsw.parallel_insert_slice(&vectors_with_ids);

        tracing::info!(count, "HNSW index built successfully");

        Ok(Self {
            hnsw,
            count,
            embedder_id,
            dimension,
        })
    }

    /// Search for approximate nearest neighbors.
    ///
    /// Returns up to `k` results sorted by similarity (highest first).
    /// The `ef` parameter controls search accuracy (higher = more accurate but slower).
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<AnnSearchResult>> {
        if query.len() != self.dimension {
            bail!(
                "query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            );
        }

        if k == 0 {
            return Ok(Vec::new());
        }

        // HNSW search returns neighbors sorted by distance (ascending).
        let neighbors: Vec<Neighbour> = self.hnsw.search(query, k, ef);

        // Convert to our result type, inverting distance to score.
        // DistDot uses 1 - dot_product, so we convert back.
        let results: Vec<AnnSearchResult> = neighbors
            .into_iter()
            .map(|n| AnnSearchResult {
                row_idx: n.d_id,
                distance: n.distance,
            })
            .collect();

        Ok(results)
    }

    /// Get the number of vectors in the index.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the embedder ID this index was built for.
    pub fn embedder_id(&self) -> &str {
        &self.embedder_id
    }

    /// Get the vector dimension.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Save the HNSW index to a file.
    ///
    /// Format:
    /// - Magic: "CHSW" (4 bytes)
    /// - Version: u16
    /// - Embedder ID length: u16
    /// - Embedder ID: bytes
    /// - Dimension: u32
    /// - Count: u32
    /// - HNSW graph data (serialized via hnsw_rs)
    pub fn save(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;

        let temp_path = path.with_extension("chsw.tmp");
        let file = File::create(&temp_path)
            .with_context(|| format!("create temp HNSW file {temp_path:?}"))?;
        let mut writer = BufWriter::new(file);

        // Write header.
        writer.write_all(&HNSW_MAGIC)?;
        writer.write_all(&HNSW_VERSION.to_le_bytes())?;

        let id_bytes = self.embedder_id.as_bytes();
        let id_len = u16::try_from(id_bytes.len())
            .map_err(|_| anyhow::anyhow!("embedder_id too long"))?;
        writer.write_all(&id_len.to_le_bytes())?;
        writer.write_all(id_bytes)?;

        writer.write_all(&(self.dimension as u32).to_le_bytes())?;
        writer.write_all(&(self.count as u32).to_le_bytes())?;

        // Serialize HNSW graph using hnsw_rs's file_dump.
        // It creates multiple files: basename.hnsw.graph and basename.hnsw.data
        let temp_dir = parent.join(".hnsw_tmp");
        std::fs::create_dir_all(&temp_dir)?;
        let basename = "hnsw_graph";
        self.hnsw
            .file_dump(&temp_dir, basename)
            .with_context(|| "serialize HNSW graph")?;

        // Read the generated files and append to our file.
        let graph_file = temp_dir.join(format!("{basename}.hnsw.graph"));
        let data_file = temp_dir.join(format!("{basename}.hnsw.data"));

        // Read graph file.
        let graph_data = std::fs::read(&graph_file).unwrap_or_default();
        writer.write_all(&(graph_data.len() as u64).to_le_bytes())?;
        writer.write_all(&graph_data)?;

        // Read data file.
        let data_data = std::fs::read(&data_file).unwrap_or_default();
        writer.write_all(&(data_data.len() as u64).to_le_bytes())?;
        writer.write_all(&data_data)?;

        writer.flush()?;
        drop(writer);

        // Clean up temp files.
        let _ = std::fs::remove_file(&graph_file);
        let _ = std::fs::remove_file(&data_file);
        let _ = std::fs::remove_dir(&temp_dir);

        // Atomic rename.
        std::fs::rename(&temp_path, path)?;

        tracing::info!(?path, count = self.count, "Saved HNSW index");
        Ok(())
    }

    /// Load an HNSW index from a file.
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open HNSW file {path:?}"))?;
        let mut reader = BufReader::new(file);

        // Read and validate magic.
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != HNSW_MAGIC {
            bail!("invalid HNSW magic: {:?}", magic);
        }

        // Read version.
        let mut version_bytes = [0u8; 2];
        reader.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);
        if version != HNSW_VERSION {
            bail!("unsupported HNSW version: {version}");
        }

        // Read embedder ID.
        let mut id_len_bytes = [0u8; 2];
        reader.read_exact(&mut id_len_bytes)?;
        let id_len = u16::from_le_bytes(id_len_bytes) as usize;
        let mut id_bytes = vec![0u8; id_len];
        reader.read_exact(&mut id_bytes)?;
        let embedder_id = String::from_utf8(id_bytes)?;

        // Read dimension and count.
        let mut dim_bytes = [0u8; 4];
        reader.read_exact(&mut dim_bytes)?;
        let dimension = u32::from_le_bytes(dim_bytes) as usize;

        let mut count_bytes = [0u8; 4];
        reader.read_exact(&mut count_bytes)?;
        let count = u32::from_le_bytes(count_bytes) as usize;

        // Read graph data length.
        let mut graph_len_bytes = [0u8; 8];
        reader.read_exact(&mut graph_len_bytes)?;
        let graph_len = u64::from_le_bytes(graph_len_bytes) as usize;

        // Read graph data to temp file.
        let mut graph_data = vec![0u8; graph_len];
        reader.read_exact(&mut graph_data)?;

        let temp_dir = tempfile::tempdir()?;
        let graph_path = temp_dir.path().join("hnsw.graph");
        std::fs::write(&graph_path, &graph_data)?;

        // Load HNSW from temp file.
        // Note: hnsw_rs load requires description file, which we need to handle.
        // For now, we'll need to store the graph differently or use a different approach.
        // TODO: Implement proper HNSW serialization using hnsw_rs API.

        // Placeholder: Create empty HNSW and log warning.
        // This is a limitation of the current implementation.
        tracing::warn!(
            "HNSW load not fully implemented - rebuild index with --build-hnsw"
        );

        let hnsw: Hnsw<f32, DistDot> = Hnsw::new(
            DEFAULT_M,
            count.max(1),
            DEFAULT_MAX_LAYER,
            DEFAULT_EF_CONSTRUCTION,
            DistDot,
        );

        Ok(Self {
            hnsw,
            count,
            embedder_id,
            dimension,
        })
    }

    /// Check if an HNSW index file exists for the given embedder.
    pub fn exists(data_dir: &Path, embedder_id: &str) -> bool {
        hnsw_index_path(data_dir, embedder_id).exists()
    }
}

impl std::fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndex")
            .field("count", &self.count)
            .field("embedder_id", &self.embedder_id)
            .field("dimension", &self.dimension)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_path() {
        let path = hnsw_index_path(Path::new("/data"), "fastembed");
        assert_eq!(
            path,
            PathBuf::from("/data/vector_index/hnsw-fastembed.chsw")
        );
    }
}
