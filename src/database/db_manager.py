import sqlite3
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import aiosqlite
from pathlib import Path
import os

class DatabaseManager:
    """Manages database operations for the P2P network."""
    
    def __init__(self, db_path: str = "p2p_network.db"):
        """Initialize the database manager."""
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self._connection = None
        self._initialize_db()

    def _initialize_db(self):
        """Initialize the database with proper schema."""
        try:
            # Check if database exists and has correct schema
            db_exists = os.path.exists(self.db_path)
            if db_exists:
                # Check if schema needs update
                with self.get_connection() as conn:
                    cursor = conn.execute("PRAGMA table_info(peers)")
                    columns = {row[1] for row in cursor.fetchall()}
                    if 'username' not in columns:
                        self.logger.info("Database schema needs update. Recreating database...")
                        conn.close()
                        os.remove(self.db_path)
                        db_exists = False
            
            # Create new database if needed
            if not db_exists:
                self.logger.info("Creating new database with updated schema...")
                self._create_tables()
                self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise

    def _create_tables(self):
        """Create necessary database tables if they don't exist."""
        with self.get_connection() as conn:
            # Create peers table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS peers (
                    id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    username TEXT,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create files table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    hash TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    owner_id TEXT NOT NULL,
                    shared BOOLEAN DEFAULT 0,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (owner_id) REFERENCES peers(id)
                )
            """)
            
            # Create file_peers table for tracking which peers have which files
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_peers (
                    file_hash TEXT,
                    peer_id TEXT,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (file_hash, peer_id),
                    FOREIGN KEY (file_hash) REFERENCES files(hash),
                    FOREIGN KEY (peer_id) REFERENCES peers(id)
                )
            """)
            
            conn.commit()

    def get_connection(self):
        """Get a database connection."""
        try:
            if self._connection is None:
                self._connection = sqlite3.connect(self.db_path)
                self._connection.row_factory = sqlite3.Row
            return self._connection
        except Exception as e:
            self.logger.error(f"Error getting database connection: {e}")
            raise

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
                    hash TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    modified_at TIMESTAMP NOT NULL,
                    owner_id TEXT NOT NULL,
                    permissions TEXT NOT NULL,
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
        """Get peer information from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            async with db.execute('SELECT * FROM peers WHERE id = ?', (peer_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    def get_peer_sync(self, peer_id: str) -> Optional[Dict]:
        """Get peer information synchronously."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM peers WHERE id = ?",
                    (peer_id,)
                )
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'host': row[1],
                        'port': row[2],
                        'username': row[3],
                        'last_seen': row[4]
                    }
                return None
        except Exception as e:
            self.logger.error(f"Error getting peer info: {e}")
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
                        "UPDATE peers SET username = ? WHERE id = ?",
                        (username, peer_id)
                    )
                else:
                    # Insert new peer
                    conn.execute(
                        "INSERT INTO peers (id, address, port, username) VALUES (?, ?, ?, ?)",
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
                (hash, name, size, created_at, modified_at, owner_id, permissions, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_hash,
                name,
                size,
                now,
                now,
                owner_id,
                json.dumps(permissions),
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
                ORDER BY modified_at DESC
            ''', (user_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_connected_peers(self) -> List[Dict]:
        """Get all connected peers."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = sqlite3.Row
            async with db.execute('''
                SELECT * FROM peers
                WHERE is_connected = 1
                ORDER BY last_seen DESC
            ''') as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows] 