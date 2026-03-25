"""Index knowledge documents into searchable chunks for BM25 retrieval."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class DocChunk:
    """A section-level chunk from a knowledge document."""
    source: str  # filename
    section: str  # heading
    content: str  # full section text
    tokens: list[str] = field(default_factory=list)


def _tokenize(text: str) -> list[str]:
    """Simple whitespace + punctuation tokenizer with lowercasing."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9_\s]", " ", text)
    return [t for t in text.split() if len(t) > 1]


def _split_markdown_sections(text: str, source: str) -> list[DocChunk]:
    """Split a markdown file into section-level chunks.

    Each heading (## or ###) starts a new chunk. Content before
    the first heading goes into a chunk with section="overview".
    """
    lines = text.split("\n")
    chunks: list[DocChunk] = []
    current_section = "overview"
    current_lines: list[str] = []

    for line in lines:
        heading_match = re.match(r"^(#{1,3})\s+(.+)", line)
        if heading_match:
            # Save previous section
            if current_lines:
                content = "\n".join(current_lines).strip()
                if content:
                    chunks.append(DocChunk(
                        source=source,
                        section=current_section,
                        content=content,
                        tokens=_tokenize(content),
                    ))
            current_section = heading_match.group(2).strip()
            current_lines = [line]
        else:
            current_lines.append(line)

    # Save last section
    if current_lines:
        content = "\n".join(current_lines).strip()
        if content:
            chunks.append(DocChunk(
                source=source,
                section=current_section,
                content=content,
                tokens=_tokenize(content),
            ))

    return chunks


def index_knowledge_dir(knowledge_dir: str | Path) -> list[DocChunk]:
    """Index all markdown files in the knowledge directory into chunks.

    Returns list of DocChunks ready for BM25 indexing.
    """
    knowledge_dir = Path(knowledge_dir)
    all_chunks: list[DocChunk] = []

    if not knowledge_dir.exists():
        logger.warning("knowledge_dir_not_found", path=str(knowledge_dir))
        return all_chunks

    for md_file in sorted(knowledge_dir.glob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        chunks = _split_markdown_sections(text, source=md_file.name)
        all_chunks.extend(chunks)
        logger.debug("indexed_file", file=md_file.name, chunks=len(chunks))

    logger.info("indexing_complete", total_chunks=len(all_chunks))
    return all_chunks
