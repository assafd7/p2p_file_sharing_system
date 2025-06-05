import pytest
import asyncio
from datetime import datetime
from src.network.dht import DHT, DHTError
from src.network.peer import PeerInfo
from typing import Optional

@pytest.mark.asyncio
async def test_dht_initialization(dht: DHT):
    """Test DHT initialization."""
    assert dht.node_id == "test_node_id"
    assert dht.host == "127.0.0.1"
    assert dht.port == 8000
    assert not dht.is_running

@pytest.mark.asyncio
async def test_dht_start_stop(dht: DHT):
    """Test DHT start and stop."""
    await dht.start()
    assert dht.is_running
    
    await dht.stop()
    assert not dht.is_running

@pytest.mark.asyncio
async def test_dht_peer_management(dht: DHT):
    """Test DHT peer management."""
    await dht.start()
    
    # Add peer
    peer = PeerInfo(
        id="test_peer",
        address="127.0.0.1",
        port=8001,
        last_seen=datetime.now(),
        is_connected=True
    )
    
    await dht.add_peer(peer)
    assert peer.id in dht.peers
    
    # Get peer
    retrieved_peer = await dht.get_peer(peer.id)
    assert retrieved_peer == peer
    
    # Remove peer
    await dht.remove_peer(peer.id)
    assert peer.id not in dht.peers
    
    await dht.stop()

@pytest.mark.asyncio
async def test_dht_peer_discovery(dht: DHT):
    """Test DHT peer discovery."""
    await dht.start()
    
    # Add multiple peers
    peers = []
    for i in range(5):
        peer = PeerInfo(
            id=f"test_peer_{i}",
            address="127.0.0.1",
            port=8001 + i,
            last_seen=datetime.now(),
            is_connected=True
        )
        peers.append(peer)
        await dht.add_peer(peer)
    
    # Test find peers
    found_peers = await dht.find_peers("test")
    assert len(found_peers) > 0
    
    # Test find closest peers
    closest_peers = await dht.find_closest_peers("test_peer_0", k=3)
    assert len(closest_peers) <= 3
    
    await dht.stop()

@pytest.mark.asyncio
async def test_dht_bootstrap(dht: DHT):
    """Test DHT bootstrap process."""
    await dht.start()
    
    # Test bootstrap with no initial peers
    await dht.bootstrap([])
    assert len(dht.peers) == 0
    
    # Test bootstrap with initial peers
    initial_peers = [
        PeerInfo(
            id=f"bootstrap_peer_{i}",
            address="127.0.0.1",
            port=9000 + i,
            last_seen=datetime.now(),
            is_connected=True
        )
        for i in range(3)
    ]
    
    await dht.bootstrap(initial_peers)
    assert len(dht.peers) > 0
    
    await dht.stop()

@pytest.mark.asyncio
async def test_dht_peer_health_check(dht: DHT):
    """Test DHT peer health checking."""
    await dht.start()
    
    # Add peer
    peer = PeerInfo(
        id="test_peer",
        address="127.0.0.1",
        port=8001,
        last_seen=datetime.now(),
        is_connected=True
    )
    
    await dht.add_peer(peer)
    
    # Test health check
    is_healthy = await dht.check_peer_health(peer.id)
    assert isinstance(is_healthy, bool)
    
    await dht.stop()

@pytest.mark.asyncio
async def test_dht_error_handling(dht: DHT):
    """Test DHT error handling."""
    await dht.start()
    
    # Test adding invalid peer
    with pytest.raises(DHTError):
        # Create an invalid peer that will fail type checking
        invalid_peer: Optional[PeerInfo] = None
        await dht.add_peer(invalid_peer)
    
    # Test getting non-existent peer
    with pytest.raises(DHTError):
        await dht.get_peer("non_existent_peer")
    
    # Test removing non-existent peer
    with pytest.raises(DHTError):
        await dht.remove_peer("non_existent_peer")
    
    await dht.stop()

@pytest.mark.asyncio
async def test_dht_concurrent_operations(dht: DHT):
    """Test concurrent DHT operations."""
    await dht.start()
    
    # Create multiple peers
    peers = [
        PeerInfo(
            id=f"test_peer_{i}",
            address="127.0.0.1",
            port=8001 + i,
            last_seen=datetime.now(),
            is_connected=True
        )
        for i in range(5)
    ]
    
    # Perform concurrent operations
    tasks = []
    for peer in peers:
        tasks.append(asyncio.create_task(dht.add_peer(peer)))
    
    await asyncio.gather(*tasks)
    
    assert len(dht.peers) == len(peers)
    
    await dht.stop()

@pytest.mark.asyncio
async def test_dht_cleanup(dht: DHT):
    """Test DHT cleanup on stop."""
    await dht.start()
    
    # Add some peers
    for i in range(5):
        peer = PeerInfo(
            id=f"test_peer_{i}",
            address="127.0.0.1",
            port=8001 + i,
            last_seen=datetime.now(),
            is_connected=True
        )
        await dht.add_peer(peer)
    
    # Stop DHT
    await dht.stop()
    
    # Check if peers are cleared
    assert len(dht.peers) == 0
    assert not dht.is_running 