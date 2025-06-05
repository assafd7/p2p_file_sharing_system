import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from typing import Generator, AsyncGenerator
from datetime import datetime

from src.network.protocol import Message, MessageType
from src.network.peer import Peer, PeerInfo
from src.network.dht import DHT
from src.network.security import SecurityManager
from src.file_management.file_manager import FileManager
from src.database.db_manager import DatabaseManager

@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def security_manager() -> SecurityManager:
    """Create a security manager instance."""
    return SecurityManager()

@pytest.fixture
def file_manager(temp_dir: Path) -> FileManager:
    """Create a file manager instance with temporary storage."""
    return FileManager(str(temp_dir / "storage"))

@pytest.fixture
def db_manager(temp_dir: Path) -> DatabaseManager:
    """Create a database manager instance with temporary database."""
    return DatabaseManager(str(temp_dir / "test.db"))

@pytest.fixture
async def initialized_db_manager(db_manager: DatabaseManager) -> AsyncGenerator[DatabaseManager, None]:
    """Create and initialize a database manager."""
    await db_manager.initialize()
    yield db_manager

@pytest.fixture
def dht(security_manager: SecurityManager) -> DHT:
    """Create a DHT instance."""
    return DHT(
        node_id="test_node_id",
        host="127.0.0.1",
        port=8000
    )

@pytest.fixture
def peer() -> Peer:
    """Create a peer instance."""
    return Peer("127.0.0.1", 8000)

@pytest.fixture
def sample_message() -> Message:
    """Create a sample message for testing."""
    return Message.create(
        MessageType.HELLO,
        "test_sender",
        {"version": "1.0"}
    )

@pytest.fixture
def sample_peer_info() -> PeerInfo:
    """Create a sample peer info for testing."""
    return PeerInfo(
        id="test_peer_id",
        address="127.0.0.1",
        port=8000,
        last_seen=datetime.now(),
        is_connected=True
    )

@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close() 