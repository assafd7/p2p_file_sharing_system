import hashlib
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Set
import logging
import asyncio
from pathlib import Path
import aiosqlite

@dataclass
class FileChunk:
    """Represents a chunk of a file."""
    index: int
    hash: str
    size: int

@dataclass
class FileMetadata:
    """Metadata for a file in the P2P network."""
    file_id: str  # Unique identifier (hash of file contents)
    name: str     # File name
    size: int     # File size in bytes
    hash: str     # SHA-256 hash of the file
    owner_id: str # Peer ID of the owner
    owner_name: str # Username of the owner
    upload_time: datetime # When the file was uploaded
    is_available: bool = True # Whether the file is currently available
    ttl: int = 10  # Time-to-live for broadcasting
    seen_by: Set[str] = None  # Set of peer IDs that have seen this metadata
    chunks: List['FileChunk'] = None  # List of file chunks
    
    def __post_init__(self):
        if self.seen_by is None:
            self.seen_by = set()
        if self.chunks is None:
            self.chunks = []
        if self.file_id is None:
            self.file_id = self.hash
    
    def to_dict(self) -> Dict:
        """Convert metadata to dictionary for serialization."""
        data = asdict(self)
        data['upload_time'] = self.upload_time.isoformat()
        data['seen_by'] = list(self.seen_by)
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FileMetadata':
        """Create metadata from dictionary."""
        if 'upload_time' in data:
            data['upload_time'] = datetime.fromisoformat(data['upload_time'])
        if 'seen_by' in data:
            data['seen_by'] = set(data['seen_by'])
        return cls(**data)
    
    def to_json(self) -> str:
        """Convert metadata to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'FileMetadata':
        """Create metadata from JSON string."""
        return cls.from_dict(json.loads(json_str))

class FileMetadataManager:
    """Manages file metadata in the P2P network."""
    
    def __init__(self, db_path: str):
        self.logger = logging.getLogger(__name__)
        self._metadata: Dict[str, FileMetadata] = {}  # file_id -> metadata
        self._lock = asyncio.Lock()  # Lock for thread-safe operations
        self._seen_metadata: Set[str] = set()  # Set of seen metadata IDs
        self.db_path = db_path
    
    async def add_metadata(self, metadata: FileMetadata) -> bool:
        """Add new file metadata."""
        async with self._lock:
            if metadata.file_id in self._metadata:
                # Update existing metadata if new one is more recent
                existing = self._metadata[metadata.file_id]
                if metadata.upload_time > existing.upload_time:
                    self._metadata[metadata.file_id] = metadata
                    return True
                return False
            else:
                self._metadata[metadata.file_id] = metadata
                return True
    
    async def get_metadata(self, file_id: str) -> Optional[FileMetadata]:
        """Get file metadata by ID."""
        async with self._lock:
            return self._metadata.get(file_id)
    
    async def get_all_metadata(self) -> List[FileMetadata]:
        """Get all file metadata."""
        try:
            self.logger.debug("Getting all metadata from database")
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("SELECT * FROM files") as cursor:
                    rows = await cursor.fetchall()
                    self.logger.debug(f"Retrieved {len(rows)} files from database")
                    
                    metadata_list = []
                    for row in rows:
                        try:
                            # Convert row to dict
                            file_data = dict(zip([col[0] for col in cursor.description], row))
                            
                            # Parse chunks
                            chunks_data = json.loads(file_data['chunks'])
                            chunks = [
                                FileChunk(
                                    index=chunk['index'],
                                    hash=chunk['hash'],
                                    size=chunk['size']
                                )
                                for chunk in chunks_data
                            ]
                            
                            # Create metadata object
                            metadata = FileMetadata(
                                file_id=file_data['hash'],
                                name=file_data['name'],
                                size=file_data['size'],
                                hash=file_data['hash'],
                                owner_id=file_data['owner_id'],
                                owner_name=file_data['owner_name'],
                                upload_time=datetime.fromisoformat(file_data['upload_time']),
                                is_available=bool(file_data['is_available']),
                                ttl=int(file_data['ttl']),
                                seen_by=set(json.loads(file_data['seen_by'])),
                                chunks=chunks
                            )
                            metadata_list.append(metadata)
                            self.logger.debug(f"Added metadata for file: {metadata.name}")
                        except Exception as e:
                            self.logger.error(f"Error parsing metadata for row: {e}")
                            continue
                    
                    self.logger.debug(f"Successfully parsed {len(metadata_list)} metadata entries")
                    return metadata_list
        except Exception as e:
            self.logger.error(f"Error getting all metadata: {e}")
            return []
    
    async def remove_metadata(self, file_id: str) -> bool:
        """Remove file metadata."""
        async with self._lock:
            if file_id in self._metadata:
                del self._metadata[file_id]
                return True
            return False
    
    async def has_seen_metadata(self, metadata: FileMetadata) -> bool:
        """Check if we've seen this metadata before."""
        async with self._lock:
            return metadata.file_id in self._seen_metadata
    
    async def mark_metadata_seen(self, metadata: FileMetadata, peer_id: str):
        """Mark metadata as seen by a peer."""
        async with self._lock:
            self._seen_metadata.add(metadata.file_id)
            if metadata.file_id in self._metadata:
                self._metadata[metadata.file_id].seen_by.add(peer_id)
    
    async def create_metadata(self, file_path: Path, owner_id: str, owner_name: str) -> FileMetadata:
        """Create metadata for a new file."""
        try:
            # Calculate file hash
            file_hash = hashlib.sha256()
            chunks = []
            chunk_index = 0
            
            with open(file_path, 'rb') as f:
                while True:
                    chunk_data = f.read(8192)
                    if not chunk_data:
                        break
                    file_hash.update(chunk_data)
                    chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                    chunks.append(FileChunk(
                        index=chunk_index,
                        hash=chunk_hash,
                        size=len(chunk_data)
                    ))
                    chunk_index += 1
            
            file_id = file_hash.hexdigest()
            
            # Create metadata
            return FileMetadata(
                file_id=file_id,
                name=file_path.name,
                size=file_path.stat().st_size,
                hash=file_id,
                owner_id=owner_id,
                owner_name=owner_name,
                upload_time=datetime.now(),
                is_available=True,
                chunks=chunks
            )
        except Exception as e:
            self.logger.error(f"Error creating metadata: {e}")
            raise
    
    async def cleanup_old_metadata(self, max_age_days: int = 30):
        """Remove metadata older than max_age_days."""
        async with self._lock:
            current_time = datetime.now()
            to_remove = []
            
            for file_id, metadata in self._metadata.items():
                age = current_time - metadata.upload_time
                if age.days > max_age_days:
                    to_remove.append(file_id)
            
            for file_id in to_remove:
                del self._metadata[file_id]
                self._seen_metadata.discard(file_id) 