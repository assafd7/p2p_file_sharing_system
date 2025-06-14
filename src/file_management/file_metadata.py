import hashlib
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Set
import logging
import asyncio
from pathlib import Path

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
    
    def __post_init__(self):
        if self.seen_by is None:
            self.seen_by = set()
    
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
    
    def __init__(self, storage_dir: str = "data/metadata"):
        self.logger = logging.getLogger(__name__)
        self._metadata: Dict[str, FileMetadata] = {}  # file_id -> metadata
        self._lock = asyncio.Lock()  # Lock for thread-safe operations
        self._seen_metadata: Set[str] = set()  # Set of seen metadata IDs
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._load_metadata()  # Load existing metadata on startup
    
    def _load_metadata(self):
        """Load metadata from disk."""
        try:
            metadata_file = self.storage_dir / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    data = json.load(f)
                    for file_id, metadata_dict in data.items():
                        self._metadata[file_id] = FileMetadata.from_dict(metadata_dict)
                self.logger.info(f"Loaded {len(self._metadata)} metadata entries from disk")
        except Exception as e:
            self.logger.error(f"Error loading metadata: {e}")
    
    def _save_metadata(self):
        """Save metadata to disk."""
        try:
            metadata_file = self.storage_dir / "metadata.json"
            data = {file_id: metadata.to_dict() for file_id, metadata in self._metadata.items()}
            with open(metadata_file, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.debug("Metadata saved to disk")
        except Exception as e:
            self.logger.error(f"Error saving metadata: {e}")
    
    async def add_metadata(self, metadata: FileMetadata) -> bool:
        """Add new file metadata."""
        async with self._lock:
            if metadata.file_id in self._metadata:
                # Update existing metadata if new one is more recent
                existing = self._metadata[metadata.file_id]
                if metadata.upload_time > existing.upload_time:
                    self._metadata[metadata.file_id] = metadata
                    self._save_metadata()
                    return True
                return False
            else:
                self._metadata[metadata.file_id] = metadata
                self._save_metadata()
                return True
    
    async def get_metadata(self, file_id: str) -> Optional[FileMetadata]:
        """Get file metadata by ID."""
        async with self._lock:
            return self._metadata.get(file_id)
    
    async def get_all_metadata(self) -> List[FileMetadata]:
        """Get all file metadata."""
        async with self._lock:
            return list(self._metadata.values())
    
    async def remove_metadata(self, file_id: str) -> bool:
        """Remove file metadata."""
        async with self._lock:
            if file_id in self._metadata:
                del self._metadata[file_id]
                self._save_metadata()
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
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    file_hash.update(chunk)
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
                is_available=True
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