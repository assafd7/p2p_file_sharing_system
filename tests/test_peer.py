import pytest
import asyncio
from datetime import datetime, timedelta
from src.network.peer import Peer, PeerInfo, PeerError
from src.network.protocol import Message, MessageType
from typing import Optional

@pytest.mark.asyncio
async def test_peer_connection(peer: Peer):
    """Test peer connection and disconnection."""
    assert not peer.is_connected()
    
    # Start the peer
    await peer.start()
    assert peer.is_connected()
    
    # Stop the peer
    await peer.stop()
    assert not peer.is_connected()

@pytest.mark.asyncio
async def test_peer_info():
    """Test peer info creation and updates."""
    info = PeerInfo(
        id="test_id",
        address="127.0.0.1",
        port=8000,
        last_seen=datetime.now(),
        is_connected=True
    )
    
    assert info.id == "test_id"
    assert info.address == "127.0.0.1"
    assert info.port == 8000
    assert info.is_connected
    
    # Test update
    new_time = datetime.now() + timedelta(minutes=5)
    info.update_last_seen(new_time)
    assert info.last_seen == new_time

@pytest.mark.asyncio
async def test_peer_message_handling(peer: Peer):
    """Test peer message handling."""
    await peer.start()
    
    # Test sending message
    message = Message.create(
        MessageType.HELLO,
        "test_sender",
        {"version": "1.0"}
    )
    
    # Since we're testing in isolation, this should raise a connection error
    with pytest.raises(PeerError):
        await peer.send_message(message)
    
    await peer.stop()

@pytest.mark.asyncio
async def test_peer_reconnection(peer: Peer):
    """Test peer reconnection logic."""
    await peer.start()
    assert peer.is_connected()
    
    # Simulate connection loss
    await peer.stop()
    assert not peer.is_connected()
    
    # Attempt reconnection
    await peer.start()
    assert peer.is_connected()
    
    await peer.stop()

@pytest.mark.asyncio
async def test_peer_heartbeat(peer: Peer):
    """Test peer heartbeat mechanism."""
    await peer.start()
    
    # Send heartbeat
    await peer.send_heartbeat()
    assert peer.last_heartbeat is not None
    
    # Check if peer is considered alive
    assert peer.is_alive()
    
    # Simulate stale heartbeat
    peer.last_heartbeat = datetime.now() - timedelta(minutes=5)
    assert not peer.is_alive()
    
    await peer.stop()

@pytest.mark.asyncio
async def test_peer_message_queue(peer: Peer):
    """Test peer message queue handling."""
    await peer.start()
    
    # Add message to queue
    message = Message.create(
        MessageType.HELLO,
        "test_sender",
        {"version": "1.0"}
    )
    
    peer.message_queue.put_nowait(message)
    assert not peer.message_queue.empty()
    
    # Process message queue
    await peer.process_message_queue()
    assert peer.message_queue.empty()
    
    await peer.stop()

@pytest.mark.asyncio
async def test_peer_error_handling(peer: Peer):
    """Test peer error handling."""
    await peer.start()
    
    # Test invalid message
    with pytest.raises(PeerError):
        # Create an invalid message that will fail type checking
        invalid_message: Optional[Message] = None
        await peer.send_message(invalid_message)
    
    # Test connection error
    await peer.stop()
    with pytest.raises(PeerError):
        await peer.send_message(Message.create(
            MessageType.HELLO,
            "test_sender",
            {}
        ))
    
    # Test reconnection after error
    await peer.start()
    assert peer.is_connected()

@pytest.mark.asyncio
async def test_peer_cleanup(peer: Peer):
    """Test peer cleanup on stop."""
    await peer.start()
    
    # Add some messages to queue
    for _ in range(5):
        peer.message_queue.put_nowait(Message.create(
            MessageType.HELLO,
            "test_sender",
            {}
        ))
    
    # Stop peer
    await peer.stop()
    
    # Check if queue is empty
    assert peer.message_queue.empty()
    assert not peer.is_connected()

@pytest.mark.asyncio
async def test_peer_concurrent_operations(peer: Peer):
    """Test concurrent peer operations."""
    await peer.start()
    
    # Create multiple tasks
    tasks = []
    for _ in range(5):
        tasks.append(asyncio.create_task(peer.send_heartbeat()))
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    assert peer.is_connected()
    await peer.stop() 