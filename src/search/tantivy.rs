use std::path::Path;

use anyhow::{Context, Result};
use tantivy::collector::TopDocs;
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, doc};

use crate::connectors::NormalizedConversation;

const SCHEMA_VERSION: &str = "v1";

#[derive(Clone, Copy)]
pub struct Fields {
    pub agent: Field,
    pub workspace: Field,
    pub source_path: Field,
    pub msg_idx: Field,
    pub created_at: Field,
    pub title: Field,
    pub content: Field,
}

pub struct TantivyIndex {
    pub index: Index,
    writer: IndexWriter,
    pub fields: Fields,
}

impl TantivyIndex {
    pub fn open_or_create(path: &Path) -> Result<Self> {
        let schema = build_schema();
        std::fs::create_dir_all(path)?;
        let index = if path.join("meta.json").exists() {
            Index::open_in_dir(path)?
        } else {
            Index::create_in_dir(path, schema.clone())?
        };
        let writer = index
            .writer(50_000_000)
            .with_context(|| "create index writer")?;

        let fields = fields_from_schema(&schema)?;

        Ok(Self {
            index,
            writer,
            fields,
        })
    }

    pub fn add_conversation(&mut self, conv: &NormalizedConversation) -> Result<()> {
        for msg in &conv.messages {
            let mut d = doc! {
                self.fields.agent => conv.agent_slug.clone(),
                self.fields.source_path => conv.source_path.to_string_lossy().into_owned(),
                self.fields.msg_idx => msg.idx as u64,
                self.fields.content => msg.content.clone(),
            };
            if let Some(ws) = &conv.workspace {
                d.add_text(self.fields.workspace, ws.to_string_lossy());
            }
            if let Some(ts) = msg.created_at.or(conv.started_at) {
                d.add_i64(self.fields.created_at, ts);
            }
            if let Some(title) = &conv.title {
                d.add_text(self.fields.title, title);
            }
            self.writer.add_document(d)?;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }

    pub fn reader(&self) -> Result<IndexReader> {
        Ok(self.index.reader()?)
    }
}

pub fn build_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("agent", TEXT | STORED);
    schema_builder.add_text_field("workspace", TEXT | STORED);
    schema_builder.add_text_field("source_path", STORED);
    schema_builder.add_u64_field("msg_idx", INDEXED | STORED);
    schema_builder.add_i64_field("created_at", INDEXED | STORED);
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("content", TEXT | STORED);
    schema_builder.build()
}

pub fn fields_from_schema(schema: &Schema) -> Result<Fields> {
    Ok(Fields {
        agent: schema
            .get_field("agent")
            .ok_or_else(|| anyhow::anyhow!("schema missing agent"))?,
        workspace: schema
            .get_field("workspace")
            .ok_or_else(|| anyhow::anyhow!("schema missing workspace"))?,
        source_path: schema
            .get_field("source_path")
            .ok_or_else(|| anyhow::anyhow!("schema missing source_path"))?,
        msg_idx: schema
            .get_field("msg_idx")
            .ok_or_else(|| anyhow::anyhow!("schema missing msg_idx"))?,
        created_at: schema
            .get_field("created_at")
            .ok_or_else(|| anyhow::anyhow!("schema missing created_at"))?,
        title: schema
            .get_field("title")
            .ok_or_else(|| anyhow::anyhow!("schema missing title"))?,
        content: schema
            .get_field("content")
            .ok_or_else(|| anyhow::anyhow!("schema missing content"))?,
    })
}

pub fn index_dir(base: &Path) -> Result<std::path::PathBuf> {
    let dir = base.join("index").join(SCHEMA_VERSION);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
