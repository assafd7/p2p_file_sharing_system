import pytest
import os
import tempfile
import shutil
from pathlib import Path
from src.file_management.file_manager import FileManager, FileManagerError

@pytest.fixture
def temp_storage():
    """Create a temporary storage directory."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def file_manager(temp_storage):
    """Create a file manager instance with temporary storage."""
    return FileManager(temp_storage)

def test_file_manager_initialization(file_manager: FileManager, temp_storage: str):
    """Test file manager initialization."""
    assert file_manager.storage_path == temp_storage
    assert os.path.exists(temp_storage)
    assert file_manager.chunk_size == 1024 * 1024  # 1MB default chunk size

def test_file_chunking(file_manager: FileManager):
    """Test file chunking functionality."""
    # Create a test file
    test_file = Path(file_manager.storage_path) / "test.txt"
    test_content = b"x" * (2 * 1024 * 1024)  # 2MB of data
    test_file.write_bytes(test_content)
    
    # Get chunks
    chunks = file_manager.get_file_chunks(str(test_file))
    assert len(chunks) == 2  # Should be 2 chunks of 1MB each
    
    # Verify chunk content
    for chunk in chunks:
        assert len(chunk) <= file_manager.chunk_size
        assert isinstance(chunk, bytes)

def test_file_verification(file_manager: FileManager):
    """Test file verification functionality."""
    # Create a test file
    test_file = Path(file_manager.storage_path) / "test.txt"
    test_content = b"test content"
    test_file.write_bytes(test_content)
    
    # Calculate hash
    file_hash = file_manager.calculate_file_hash(str(test_file))
    assert isinstance(file_hash, str)
    assert len(file_hash) > 0
    
    # Verify file
    assert file_manager.verify_file(str(test_file), file_hash)
    
    # Test with invalid hash
    assert not file_manager.verify_file(str(test_file), "invalid_hash")

def test_file_transfer(file_manager: FileManager):
    """Test file transfer functionality."""
    # Create source and destination files
    source_file = Path(file_manager.storage_path) / "source.txt"
    dest_file = Path(file_manager.storage_path) / "dest.txt"
    
    # Write test content
    test_content = b"test content for transfer"
    source_file.write_bytes(test_content)
    
    # Transfer file
    file_manager.transfer_file(str(source_file), str(dest_file))
    
    # Verify transfer
    assert dest_file.exists()
    assert dest_file.read_bytes() == test_content

def test_file_cleanup(file_manager: FileManager):
    """Test file cleanup functionality."""
    # Create test files
    test_files = []
    for i in range(5):
        file_path = Path(file_manager.storage_path) / f"test_{i}.txt"
        file_path.write_bytes(b"test content")
        test_files.append(str(file_path))
    
    # Clean up files
    file_manager.cleanup_files(test_files)
    
    # Verify cleanup
    for file_path in test_files:
        assert not os.path.exists(file_path)

def test_file_resume(file_manager: FileManager):
    """Test file resume functionality."""
    # Create a test file
    test_file = Path(file_manager.storage_path) / "test.txt"
    test_content = b"x" * (2 * 1024 * 1024)  # 2MB of data
    test_file.write_bytes(test_content)
    
    # Get transfer info
    transfer_info = file_manager.get_transfer_info(str(test_file))
    assert transfer_info is not None
    assert "chunks" in transfer_info
    assert "hash" in transfer_info
    
    # Test resume with partial transfer
    partial_chunks = transfer_info["chunks"][:1]  # Only first chunk
    resumed_info = file_manager.resume_transfer(str(test_file), partial_chunks)
    assert resumed_info is not None
    assert len(resumed_info["remaining_chunks"]) == len(transfer_info["chunks"]) - 1

def test_error_handling(file_manager: FileManager):
    """Test error handling."""
    # Test non-existent file
    with pytest.raises(FileManagerError):
        file_manager.get_file_chunks("non_existent_file.txt")
    
    # Test invalid file path
    with pytest.raises(FileManagerError):
        file_manager.transfer_file("invalid/path", "dest.txt")
    
    # Test invalid hash
    with pytest.raises(FileManagerError):
        file_manager.verify_file("test.txt", "invalid_hash")

def test_concurrent_operations(file_manager: FileManager):
    """Test concurrent file operations."""
    import asyncio
    
    async def write_file(file_path: str, content: bytes):
        Path(file_path).write_bytes(content)
    
    async def read_file(file_path: str) -> bytes:
        return Path(file_path).read_bytes()
    
    # Create multiple test files
    test_files = []
    for i in range(5):
        file_path = str(Path(file_manager.storage_path) / f"test_{i}.txt")
        test_files.append(file_path)
    
    # Perform concurrent writes
    async def test_concurrent_writes():
        tasks = []
        for i, file_path in enumerate(test_files):
            content = f"test content {i}".encode()
            tasks.append(write_file(file_path, content))
        await asyncio.gather(*tasks)
    
    # Perform concurrent reads
    async def test_concurrent_reads():
        tasks = []
        for file_path in test_files:
            tasks.append(read_file(file_path))
        results = await asyncio.gather(*tasks)
        return results
    
    # Run tests
    asyncio.run(test_concurrent_writes())
    results = asyncio.run(test_concurrent_reads())
    
    # Verify results
    assert len(results) == len(test_files)
    for i, content in enumerate(results):
        assert content == f"test content {i}".encode()

def test_large_file_handling(file_manager: FileManager):
    """Test handling of large files."""
    # Create a large test file (100MB)
    test_file = Path(file_manager.storage_path) / "large_test.txt"
    chunk_size = 1024 * 1024  # 1MB
    num_chunks = 100
    
    # Write file in chunks
    with test_file.open("wb") as f:
        for _ in range(num_chunks):
            f.write(b"x" * chunk_size)
    
    # Get chunks
    chunks = file_manager.get_file_chunks(str(test_file))
    assert len(chunks) == num_chunks
    
    # Verify each chunk
    for chunk in chunks:
        assert len(chunk) <= file_manager.chunk_size
        assert isinstance(chunk, bytes)
    
    # Calculate hash
    file_hash = file_manager.calculate_file_hash(str(test_file))
    assert isinstance(file_hash, str)
    
    # Verify file
    assert file_manager.verify_file(str(test_file), file_hash) 