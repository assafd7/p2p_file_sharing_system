import pytest
import os
import tempfile
import shutil
from datetime import datetime
from src.database.db_manager import DatabaseManager, DatabaseError
from typing import Optional, Dict

@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.db")
    yield db_path
    shutil.rmtree(temp_dir)

@pytest.fixture
async def db_manager(temp_db):
    """Create and initialize a database manager instance."""
    manager = DatabaseManager(temp_db)
    await manager.initialize()
    yield manager
    await manager.close()

@pytest.mark.asyncio
async def test_db_initialization(db_manager: DatabaseManager, temp_db: str):
    """Test database initialization."""
    assert db_manager.db_path == temp_db
    assert os.path.exists(temp_db)
    
    # Verify tables exist
    async with db_manager.get_connection() as conn:
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = await cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        assert "files" in table_names
        assert "transfers" in table_names
        assert "peers" in table_names

@pytest.mark.asyncio
async def test_file_operations(db_manager: DatabaseManager):
    """Test file operations."""
    # Add file
    file_id = "test_file_1"
    file_info = {
        "name": "test.txt",
        "size": 1024,
        "hash": "test_hash",
        "chunks": 2,
        "status": "available"
    }
    
    await db_manager.add_file(file_id, file_info)
    
    # Get file
    retrieved_file = await db_manager.get_file(file_id)
    assert retrieved_file is not None
    assert retrieved_file["name"] == file_info["name"]
    assert retrieved_file["size"] == file_info["size"]
    
    # Update file
    new_status = "transferring"
    await db_manager.update_file_status(file_id, new_status)
    
    updated_file = await db_manager.get_file(file_id)
    assert updated_file["status"] == new_status
    
    # Delete file
    await db_manager.delete_file(file_id)
    deleted_file = await db_manager.get_file(file_id)
    assert deleted_file is None

@pytest.mark.asyncio
async def test_transfer_operations(db_manager: DatabaseManager):
    """Test transfer operations."""
    # Add transfer
    transfer_id = "test_transfer_1"
    transfer_info = {
        "file_id": "test_file_1",
        "peer_id": "test_peer_1",
        "direction": "download",
        "status": "in_progress",
        "progress": 0.5
    }
    
    await db_manager.add_transfer(transfer_id, transfer_info)
    
    # Get transfer
    retrieved_transfer = await db_manager.get_transfer(transfer_id)
    assert retrieved_transfer is not None
    assert retrieved_transfer["file_id"] == transfer_info["file_id"]
    assert retrieved_transfer["direction"] == transfer_info["direction"]
    
    # Update transfer
    new_progress = 0.75
    await db_manager.update_transfer_progress(transfer_id, new_progress)
    
    updated_transfer = await db_manager.get_transfer(transfer_id)
    assert updated_transfer["progress"] == new_progress
    
    # Complete transfer
    await db_manager.complete_transfer(transfer_id)
    completed_transfer = await db_manager.get_transfer(transfer_id)
    assert completed_transfer["status"] == "completed"

@pytest.mark.asyncio
async def test_peer_operations(db_manager: DatabaseManager):
    """Test peer operations."""
    # Add peer
    peer_id = "test_peer_1"
    peer_info = {
        "address": "127.0.0.1",
        "port": 8000,
        "last_seen": datetime.now().isoformat(),
        "status": "online"
    }
    
    await db_manager.add_peer(peer_id, peer_info)
    
    # Get peer
    retrieved_peer = await db_manager.get_peer(peer_id)
    assert retrieved_peer is not None
    assert retrieved_peer["address"] == peer_info["address"]
    assert retrieved_peer["port"] == peer_info["port"]
    
    # Update peer
    new_status = "offline"
    await db_manager.update_peer_status(peer_id, new_status)
    
    updated_peer = await db_manager.get_peer(peer_id)
    assert updated_peer["status"] == new_status
    
    # Remove peer
    await db_manager.remove_peer(peer_id)
    removed_peer = await db_manager.get_peer(peer_id)
    assert removed_peer is None

@pytest.mark.asyncio
async def test_query_operations(db_manager: DatabaseManager):
    """Test query operations."""
    # Add test data
    files = [
        ("file1", {"name": "test1.txt", "size": 1000, "status": "available"}),
        ("file2", {"name": "test2.txt", "size": 2000, "status": "transferring"}),
        ("file3", {"name": "test3.txt", "size": 3000, "status": "available"})
    ]
    
    for file_id, file_info in files:
        await db_manager.add_file(file_id, file_info)
    
    # Test search files
    results = await db_manager.search_files("test")
    assert len(results) == 3
    
    # Test get available files
    available = await db_manager.get_available_files()
    assert len(available) == 2
    
    # Test get active transfers
    transfers = [
        ("transfer1", {"file_id": "file1", "status": "in_progress"}),
        ("transfer2", {"file_id": "file2", "status": "completed"}),
        ("transfer3", {"file_id": "file3", "status": "in_progress"})
    ]
    
    for transfer_id, transfer_info in transfers:
        await db_manager.add_transfer(transfer_id, transfer_info)
    
    active = await db_manager.get_active_transfers()
    assert len(active) == 2

@pytest.mark.asyncio
async def test_error_handling(db_manager: DatabaseManager):
    """Test error handling."""
    # Test non-existent file
    with pytest.raises(DatabaseError):
        await db_manager.get_file("non_existent_file")
    
    # Test invalid file info
    with pytest.raises(DatabaseError):
        # Create invalid file info that will fail type checking
        invalid_file_info: Optional[Dict] = None
        await db_manager.add_file("test_file", invalid_file_info)
    
    # Test invalid transfer info
    with pytest.raises(DatabaseError):
        # Create invalid transfer info that will fail type checking
        invalid_transfer_info: Optional[Dict] = None
        await db_manager.add_transfer("test_transfer", invalid_transfer_info)
    
    # Test invalid peer info
    with pytest.raises(DatabaseError):
        # Create invalid peer info that will fail type checking
        invalid_peer_info: Optional[Dict] = None
        await db_manager.add_peer("test_peer", invalid_peer_info)

@pytest.mark.asyncio
async def test_concurrent_operations(db_manager: DatabaseManager):
    """Test concurrent database operations."""
    import asyncio
    
    async def add_file(file_id: str):
        await db_manager.add_file(file_id, {
            "name": f"test_{file_id}.txt",
            "size": 1000,
            "status": "available"
        })
    
    async def add_transfer(transfer_id: str):
        await db_manager.add_transfer(transfer_id, {
            "file_id": f"file_{transfer_id}",
            "status": "in_progress"
        })
    
    # Create multiple tasks
    tasks = []
    for i in range(5):
        tasks.append(add_file(f"file_{i}"))
        tasks.append(add_transfer(f"transfer_{i}"))
    
    # Execute tasks concurrently
    await asyncio.gather(*tasks)
    
    # Verify results
    files = await db_manager.search_files("test_")
    assert len(files) == 5
    
    transfers = await db_manager.get_active_transfers()
    assert len(transfers) == 5

@pytest.mark.asyncio
async def test_transaction_handling(db_manager: DatabaseManager):
    """Test transaction handling."""
    # Start transaction
    async with db_manager.transaction() as transaction:
        # Add file
        await transaction.add_file("test_file", {
            "name": "test.txt",
            "size": 1000,
            "status": "available"
        })
        
        # Add transfer
        await transaction.add_transfer("test_transfer", {
            "file_id": "test_file",
            "status": "in_progress"
        })
    
    # Verify transaction was committed
    file = await db_manager.get_file("test_file")
    assert file is not None
    
    transfer = await db_manager.get_transfer("test_transfer")
    assert transfer is not None
    
    # Test rollback
    try:
        async with db_manager.transaction() as transaction:
            await transaction.add_file("rollback_file", {
                "name": "rollback.txt",
                "size": 1000,
                "status": "available"
            })
            raise Exception("Simulated error")
    except Exception:
        pass
    
    # Verify rollback
    file = await db_manager.get_file("rollback_file")
    assert file is None 