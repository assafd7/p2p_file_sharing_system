import sqlite3
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import aiosqlite
from pathlib import Path
from dataclasses import asdict

class DatabaseManager:
    def __init__(self, db_path: str = "p2p_network.db"):
        """Initialize the database manager."""
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self._connection = None
        self._migrate_database()

    def _migrate_database(self):
        """Handle database migrations."""
        with self.get_connection() as conn:
            # Always create tables if they do not exist first
            conn.execute("""
                CREATE TABLE IF NOT EXISTS peers (
                    id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    username TEXT,
                    is_connected BOOLEAN DEFAULT 1,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    hash TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    owner_name TEXT NOT NULL,
                    upload_time TIMESTAMP NOT NULL,
                    is_available BOOLEAN NOT NULL DEFAULT 1,
                    ttl INTEGER NOT NULL DEFAULT 10,
                    seen_by TEXT,
                    chunks TEXT,
                    metadata TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    file_hash TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    hash TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY (file_hash, chunk_index),
                    FOREIGN KEY (file_hash) REFERENCES files(hash)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transfers (
                    id TEXT PRIMARY KEY,
                    file_hash TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress REAL NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (file_hash) REFERENCES files(hash)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    last_login TIMESTAMP,
                    is_active BOOLEAN NOT NULL,
                    metadata TEXT
                )
            """)
            conn.commit()

            # Clean up any existing temporary tables from failed migrations
            conn.execute("DROP TABLE IF EXISTS files_new")
            conn.execute("DROP TABLE IF EXISTS peers_new")
            conn.commit()

            # Now safe to run PRAGMA and ALTER TABLE on existing tables
            cursor = conn.execute("PRAGMA table_info(peers)")
            columns = {row[1] for row in cursor.fetchall()}
            if 'username' not in columns:
                self.logger.info("Adding username column to peers table")
                conn.execute("ALTER TABLE peers ADD COLUMN username TEXT")
                conn.commit()
            if 'is_connected' not in columns:
                self.logger.info("Adding is_connected column to peers table")
                conn.execute("ALTER TABLE peers ADD COLUMN is_connected BOOLEAN DEFAULT 1")
                conn.commit()
            if 'address' not in columns and 'host' in columns:
                self.logger.info("Renaming host column to address in peers table")
                conn.execute("""
                    CREATE TABLE peers_new (
                        id TEXT PRIMARY KEY,
                        address TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        username TEXT,
                        is_connected BOOLEAN DEFAULT 1,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    INSERT INTO peers_new (id, address, port, username, is_connected, last_seen)
                    SELECT id, host, port, username, 1, last_seen FROM peers
                """)
                conn.execute("DROP TABLE peers")
                conn.execute("ALTER TABLE peers_new RENAME TO peers")
                conn.commit()

            # Repeat for files table
            cursor = conn.execute("PRAGMA table_info(files)")
            columns = {row[1] for row in cursor.fetchall()}
            if 'file_id' not in columns:
                self.logger.info("Updating files table schema")
                try:
                    conn.execute("""
                        CREATE TABLE files_new (
                            file_id TEXT PRIMARY KEY,
                            name TEXT NOT NULL,
                            size INTEGER NOT NULL,
                            hash TEXT NOT NULL,
                            owner_id TEXT NOT NULL,
                            owner_name TEXT NOT NULL,
                            upload_time TIMESTAMP NOT NULL,
                            is_available BOOLEAN NOT NULL DEFAULT 1,
                            ttl INTEGER NOT NULL DEFAULT 10,
                            seen_by TEXT,
                            chunks TEXT,
                            metadata TEXT
                        )
                    """)
                    if 'hash' in columns:
                        try:
                            conn.execute("""
                                INSERT INTO files_new (
                                    file_id, name, size, hash, owner_id, owner_name,
                                    upload_time, is_available, ttl, seen_by, chunks, metadata
                                )
                                SELECT 
                                    hash, name, size, hash, owner_id, 
                                    COALESCE(owner_name, 'Unknown'), 
                                    COALESCE(upload_time, CURRENT_TIMESTAMP),
                                    1, 10, '[]', '[]', '{}'
                                FROM files
                            """)
                        except sqlite3.OperationalError as e:
                            self.logger.error(f"Error copying data to new files table: {e}")
                            conn.execute("DROP TABLE IF EXISTS files_new")
                            conn.execute("""
                                CREATE TABLE files_new (
                                    file_id TEXT PRIMARY KEY,
                                    name TEXT NOT NULL,
                                    size INTEGER NOT NULL,
                                    hash TEXT NOT NULL,
                                    owner_id TEXT NOT NULL,
                                    owner_name TEXT NOT NULL,
                                    upload_time TIMESTAMP NOT NULL,
                                    is_available BOOLEAN NOT NULL DEFAULT 1,
                                    ttl INTEGER NOT NULL DEFAULT 10,
                                    seen_by TEXT,
                                    chunks TEXT,
                                    metadata TEXT
                                )
                            """)
                            conn.execute("""
                                INSERT INTO files_new (
                                    file_id, name, size, hash, owner_id, owner_name,
                                    upload_time, is_available, ttl, seen_by, chunks, metadata
                                )
                                SELECT 
                                    hash, name, size, hash, owner_id, 
                                    'Unknown', 
                                    CURRENT_TIMESTAMP,
                                    1, 10, '[]', '[]', '{}'
                                FROM files
                            """)
                    conn.execute("DROP TABLE IF EXISTS files")
                    conn.execute("ALTER TABLE files_new RENAME TO files")
                    conn.commit()
                except sqlite3.OperationalError as e:
                    self.logger.error(f"Error during files table migration: {e}")
                    conn.execute("DROP TABLE IF EXISTS files_new")
                    conn.commit()
                    raise

    def get_connection(self):
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        # Set timezone to local time
        conn.execute("PRAGMA timezone = 'localtime'")
        return conn

    def close(self):
        """Close the database connection."""
        try:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
        except Exception as e:
            self.logger.error(f"Error closing database connection: {e}")

    async def initialize(self):
        """Initialize the database with required tables."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Create peers table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS peers (
                        id TEXT PRIMARY KEY,
                        address TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        last_seen TIMESTAMP NOT NULL,
                        is_connected BOOLEAN NOT NULL,
                        username TEXT,
                        metadata TEXT
                    )
                ''')

                # Create files table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS files (
                        file_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        hash TEXT NOT NULL,
                        owner_id TEXT NOT NULL,
                        owner_name TEXT NOT NULL,
                        upload_time TIMESTAMP NOT NULL,
                        is_available BOOLEAN NOT NULL DEFAULT 1,
                        ttl INTEGER NOT NULL DEFAULT 10,
                        seen_by TEXT,
                        chunks TEXT,
                        metadata TEXT
                    )
                ''')

                # Create chunks table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS chunks (
                        file_hash TEXT NOT NULL,
                        chunk_index INTEGER NOT NULL,
                        hash TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        PRIMARY KEY (file_hash, chunk_index),
                        FOREIGN KEY (file_hash) REFERENCES files(hash)
                    )
                ''')

                # Create transfers table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS transfers (
                        id TEXT PRIMARY KEY,
                        file_hash TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        target_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        progress REAL NOT NULL,
                        started_at TIMESTAMP NOT NULL,
                        completed_at TIMESTAMP,
                        FOREIGN KEY (file_hash) REFERENCES files(hash)
                    )
                ''')

                # Create users table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id TEXT PRIMARY KEY,
                        username TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        last_login TIMESTAMP,
                        is_active BOOLEAN NOT NULL,
                        metadata TEXT
                    )
                ''')

                # Create indexes
                await db.execute('CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner_id)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_chunks_file ON chunks(file_hash)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_transfers_file ON transfers(file_hash)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status)')

                await db.commit()
                self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise

    async def add_peer(self, peer_id: str, address: str, port: int, username: str = None, metadata: Optional[Dict] = None):
        """Add or update a peer in the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR REPLACE INTO peers (id, address, port, last_seen, is_connected, username, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                peer_id,
                address,
                port,
                datetime.now().isoformat(),
                True,
                username,
                json.dumps(metadata or {})
            ))
            await db.commit()

    async def get_peer(self, peer_id: str) -> Optional[Dict]:
        """Get peer information."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT id, address, port, username, is_connected, last_seen
                    FROM peers
                    WHERE id = ?
                    """,
                    (peer_id,)
                )
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'address': row[1],
                        'port': row[2],
                        'username': row[3],
                        'is_connected': bool(row[4]),
                        'last_seen': row[5]
                    }
                return None
        except Exception as e:
            self.logger.error(f"Error getting peer: {e}")
            return None

    def get_peer_sync(self, peer_id: str) -> Optional[Dict]:
        """Get peer information synchronously."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT id, address, port, username, is_connected,
                           datetime(last_seen, 'localtime') as last_seen
                    FROM peers
                    WHERE id = ?
                    """,
                    (peer_id,)
                )
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'host': row[1],
                        'port': row[2],
                        'username': row[3],
                        'is_connected': bool(row[4]),
                        'last_seen': row[5]
                    }
                return None
        except Exception as e:
            self.logger.error(f"Error getting peer: {e}")
            return None

    async def update_peer_status(self, peer_id: str, is_connected: bool):
        """Update peer connection status."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE peers
                SET is_connected = ?, last_seen = ?
                WHERE id = ?
            ''', (is_connected, datetime.now().isoformat(), peer_id))
            await db.commit()

    async def update_peer_username(self, peer_id: str, username: str) -> None:
        """Update a peer's username."""
        try:
            # Extract address and port from peer_id (format: "address:port")
            address, port = peer_id.split(':')
            port = int(port)
            
            with self.get_connection() as conn:
                # First check if peer exists
                cursor = conn.execute(
                    "SELECT id FROM peers WHERE id = ?",
                    (peer_id,)
                )
                peer = cursor.fetchone()
                
                if peer:
                    # Update existing peer
                    conn.execute(
                        """
                        UPDATE peers 
                        SET username = ?,
                            is_connected = 1,
                            last_seen = datetime('now', 'localtime')
                        WHERE id = ?
                        """,
                        (username, peer_id)
                    )
                else:
                    # Insert new peer
                    conn.execute(
                        """
                        INSERT INTO peers (
                            id, address, port, username, 
                            is_connected, last_seen
                        ) VALUES (?, ?, ?, ?, 1, datetime('now', 'localtime'))
                        """,
                        (peer_id, address, port, username)
                    )
                conn.commit()
                self.logger.info(f"Updated username for peer {peer_id} to {username}")
        except Exception as e:
            self.logger.error(f"Error updating peer username: {e}")
            raise

    async def add_file(self, file_hash: str, name: str, size: int, owner_id: str,
                      permissions: Dict[str, List[str]], metadata: Optional[Dict] = None):
        """Add a file to the database."""
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.now().isoformat()
            await db.execute('''
                INSERT OR REPLACE INTO files
                (file_id, name, size, hash, owner_id, owner_name,
                upload_time, is_available, ttl, seen_by, chunks, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_hash,  # Using hash as file_id
                name,
                size,
                file_hash,
                owner_id,
                "Unknown",  # Default owner name
                now,
                1,  # is_available
                10,  # ttl
                "[]",  # seen_by
                "[]",  # chunks
                json.dumps(metadata or {})
            ))
            await db.commit()

    async def add_chunk(self, file_hash: str, chunk_index: int, chunk_hash: str,
                       size: int, status: str = "pending"):
        """Add a file chunk to the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR REPLACE INTO chunks
                (file_hash, chunk_index, hash, size, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (file_hash, chunk_index, chunk_hash, size, status))
            await db.commit()

    async def update_chunk_status(self, file_hash: str, chunk_index: int, status: str):
        """Update chunk status."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE chunks
                SET status = ?
                WHERE file_hash = ? AND chunk_index = ?
            ''', (status, file_hash, chunk_index))
            await db.commit()

    async def add_transfer(self, transfer_id: str, file_hash: str, source_id: str,
                          target_id: str):
        """Add a new file transfer to the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO transfers
                (id, file_hash, source_id, target_id, status, progress, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                transfer_id,
                file_hash,
                source_id,
                target_id,
                "pending",
                0.0,
                datetime.now().isoformat()
            ))
            await db.commit()

    async def update_transfer_status(self, transfer_id: str, status: str,
                                   progress: float = None):
        """Update transfer status and progress."""
        async with aiosqlite.connect(self.db_path) as db:
            if progress is not None:
                await db.execute('''
                    UPDATE transfers
                    SET status = ?, progress = ?
                    WHERE id = ?
                ''', (status, progress, transfer_id))
            else:
                await db.execute('''
                    UPDATE transfers
                    SET status = ?
                    WHERE id = ?
                ''', (status, transfer_id))

            if status in ["completed", "failed"]:
                await db.execute('''
                    UPDATE transfers
                    SET completed_at = ?
                    WHERE id = ?
                ''', (datetime.now().isoformat(), transfer_id))

            await db.commit()

    async def get_transfer(self, transfer_id: str) -> Optional[Dict]:
        """Get transfer information."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            async with db.execute('SELECT * FROM transfers WHERE id = ?',
                                (transfer_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def add_user(self, user_id: str, username: str, password_hash: str,
                      metadata: Optional[Dict] = None):
        """Add a new user to the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO users
                (id, username, password_hash, created_at, is_active, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                user_id,
                username,
                password_hash,
                datetime.now().isoformat(),
                True,
                json.dumps(metadata or {})
            ))
            await db.commit()

    async def get_user(self, username: str) -> Optional[Dict]:
        """Get user information by username."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            async with db.execute('SELECT * FROM users WHERE username = ?',
                                (username,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def update_user_login(self, user_id: str):
        """Update user's last login timestamp."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE users
                SET last_login = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), user_id))
            await db.commit()

    async def get_active_transfers(self) -> List[Dict]:
        """Get all active transfers."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            async with db.execute('''
                SELECT * FROM transfers
                WHERE status IN ('pending', 'in_progress')
                ORDER BY started_at DESC
            ''') as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_user_files(self, user_id: str) -> List[Dict]:
        """Get all files owned by a user."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            async with db.execute('''
                SELECT * FROM files
                WHERE owner_id = ?
                ORDER BY upload_time DESC
            ''', (user_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_connected_peers(self) -> List[Dict]:
        """Get a list of currently connected peers."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT * FROM peers WHERE is_connected = 1"
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def store_file_metadata(self, metadata: 'FileMetadata') -> None:
        """Store file metadata in the database."""
        try:
            self.logger.debug(f"[store_file_metadata] DB path: {self.db_path} (absolute: {Path(self.db_path).resolve()})")
            self.logger.debug(f"[store_file_metadata] About to insert metadata for file: {metadata.name} (ID: {metadata.file_id})")
            async with aiosqlite.connect(self.db_path) as db:
                metadata_dict = metadata.to_dict()
                await db.execute('''
                    INSERT OR REPLACE INTO files (
                        file_id, name, size, hash, owner_id, owner_name,
                        upload_time, is_available, ttl, seen_by, chunks, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metadata.file_id,
                    metadata.name,
                    metadata.size,
                    metadata.hash,
                    metadata.owner_id,
                    metadata.owner_name,
                    metadata.upload_time.isoformat(),
                    metadata.is_available,
                    metadata.ttl,
                    json.dumps(list(metadata.seen_by)),
                    json.dumps([asdict(chunk) for chunk in metadata.chunks]),
                    json.dumps(metadata_dict)
                ))
                await db.commit()
                self.logger.debug(f"[store_file_metadata] Successfully inserted metadata for file: {metadata.name} (ID: {metadata.file_id})")
                self.logger.info(f"Stored metadata for file {metadata.name}")
        except Exception as e:
            self.logger.error(f"[store_file_metadata] Error storing file metadata for {metadata.name}: {e}", exc_info=True)
            raise

    async def get_file_metadata(self, file_id: str) -> Optional['FileMetadata']:
        """Get file metadata from the database."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    "SELECT * FROM files WHERE file_id = ?",
                    (file_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        # Convert row to dict
                        file_data = dict(zip([col[0] for col in cursor.description], row))
                        
                        # Parse JSON fields
                        seen_by = json.loads(file_data['seen_by']) if file_data['seen_by'] else []
                        chunks_data = json.loads(file_data['chunks']) if file_data['chunks'] else []
                        
                        # Create FileMetadata object
                        from src.file_management.file_metadata import FileMetadata, FileChunk
                        return FileMetadata(
                            file_id=file_data['file_id'],
                            name=file_data['name'],
                            size=file_data['size'],
                            hash=file_data['hash'],
                            owner_id=file_data['owner_id'],
                            owner_name=file_data['owner_name'],
                            upload_time=datetime.fromisoformat(file_data['upload_time']),
                            is_available=bool(file_data['is_available']),
                            ttl=int(file_data['ttl']),
                            seen_by=set(seen_by),
                            chunks=[FileChunk(**chunk) for chunk in chunks_data]
                        )
                    return None
        except Exception as e:
            self.logger.error(f"Error retrieving file metadata: {e}")
            return None

    async def get_all_files(self) -> List[Dict]:
        """Get all files from the database"""
        self.logger.debug("[get_all_files] Starting get_all_files")
        try:
            async with aiosqlite.connect(self.db_path) as db:
                self.logger.debug("[get_all_files] Connected to database")
                query = "SELECT * FROM files"
                self.logger.debug(f"[get_all_files] Executing query: {query}")
                async with db.execute(query) as cursor:
                    rows = await cursor.fetchall()
                    self.logger.debug(f"[get_all_files] Retrieved {len(rows)} rows from database")
                    columns = [description[0] for description in cursor.description]
                    self.logger.debug(f"[get_all_files] Column names: {columns}")
                    files = []
                    for row in rows:
                        try:
                            file_dict = dict(zip(columns, row))
                            self.logger.debug(f"[get_all_files] Row: {file_dict}")
                            if 'metadata' in file_dict and file_dict['metadata']:
                                file_dict['metadata'] = json.loads(file_dict['metadata'])
                            if 'chunks' in file_dict and file_dict['chunks']:
                                file_dict['chunks'] = json.loads(file_dict['chunks'])
                            if 'seen_by' in file_dict and file_dict['seen_by']:
                                file_dict['seen_by'] = json.loads(file_dict['seen_by'])
                            files.append(file_dict)
                        except Exception as e:
                            self.logger.error(f"[get_all_files] Error processing row: {str(e)}", exc_info=True)
                            continue
                    self.logger.debug(f"[get_all_files] Returning {len(files)} files")
                    return files
        except Exception as e:
            self.logger.error(f"[get_all_files] Error in get_all_files: {str(e)}", exc_info=True)
            return []

    def get_all_files_sync(self) -> List[Dict]:
        """Get all files from the database synchronously"""
        self.logger.debug("[get_all_files_sync] Starting get_all_files_sync")
        try:
            with sqlite3.connect(self.db_path) as db:
                self.logger.debug("[get_all_files_sync] Connected to database")
                query = "SELECT * FROM files"
                self.logger.debug(f"[get_all_files_sync] Executing query: {query}")
                cursor = db.execute(query)
                rows = cursor.fetchall()
                self.logger.debug(f"[get_all_files_sync] Retrieved {len(rows)} rows from database")
                columns = [description[0] for description in cursor.description]
                self.logger.debug(f"[get_all_files_sync] Column names: {columns}")
                files = []
                for row in rows:
                    try:
                        file_dict = dict(zip(columns, row))
                        self.logger.debug(f"[get_all_files_sync] Row: {file_dict}")
                        if 'metadata' in file_dict and file_dict['metadata']:
                            file_dict['metadata'] = json.loads(file_dict['metadata'])
                        if 'chunks' in file_dict and file_dict['chunks']:
                            file_dict['chunks'] = json.loads(file_dict['chunks'])
                        if 'seen_by' in file_dict and file_dict['seen_by']:
                            file_dict['seen_by'] = json.loads(file_dict['seen_by'])
                        files.append(file_dict)
                    except Exception as e:
                        self.logger.error(f"[get_all_files_sync] Error processing row: {str(e)}", exc_info=True)
                        continue
                self.logger.debug(f"[get_all_files_sync] Returning {len(files)} files")
                return files
        except Exception as e:
            self.logger.error(f"[get_all_files_sync] Error in get_all_files_sync: {str(e)}", exc_info=True)
            return [] 

    async def delete_file_metadata(self, file_id: str) -> bool:
        """Delete file metadata from the database."""
        try:
            self.logger.debug(f"[delete_file_metadata] Deleting file metadata for file_id: {file_id}")
            async with aiosqlite.connect(self.db_path) as db:
                # Delete from files table
                cursor = await db.execute(
                    "DELETE FROM files WHERE file_id = ?",
                    (file_id,)
                )
                deleted_count = cursor.rowcount
                await db.commit()
                
                if deleted_count > 0:
                    self.logger.debug(f"[delete_file_metadata] Successfully deleted file metadata for file_id: {file_id}")
                    return True
                else:
                    self.logger.warning(f"[delete_file_metadata] No file found with file_id: {file_id}")
                    return False
        except Exception as e:
            self.logger.error(f"[delete_file_metadata] Error deleting file metadata for file_id {file_id}: {e}", exc_info=True)
            return False 