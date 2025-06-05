import pytest
from src.network.protocol import Message, MessageType, ProtocolError

def test_message_creation():
    """Test message creation and serialization."""
    message = Message.create(
        MessageType.HELLO,
        "test_sender",
        {"version": "1.0"}
    )
    
    assert message.type == MessageType.HELLO
    assert message.sender == "test_sender"
    assert message.data == {"version": "1.0"}
    assert message.timestamp is not None

def test_message_serialization():
    """Test message serialization and deserialization."""
    original = Message.create(
        MessageType.HELLO,
        "test_sender",
        {"version": "1.0"}
    )
    
    serialized = original.serialize()
    deserialized = Message.deserialize(serialized)
    
    assert deserialized.type == original.type
    assert deserialized.sender == original.sender
    assert deserialized.data == original.data
    assert deserialized.timestamp == original.timestamp

def test_invalid_message_type():
    """Test handling of invalid message type."""
    with pytest.raises(ProtocolError):
        # Create an invalid message type that will fail type checking
        invalid_type: str = "INVALID_TYPE"
        Message.create(
            invalid_type,
            "test_sender",
            {}
        )

def test_message_validation():
    """Test message validation."""
    # Test missing required fields
    with pytest.raises(ProtocolError):
        Message(
            type=MessageType.HELLO,
            sender="",
            data={},
            timestamp=None
        )
    
    # Test invalid data type
    with pytest.raises(ProtocolError):
        # Create invalid data that will fail type checking
        invalid_data: str = "invalid_data"
        Message.create(
            MessageType.HELLO,
            "test_sender",
            invalid_data
        )

def test_message_types():
    """Test all message types."""
    for msg_type in MessageType:
        message = Message.create(
            msg_type,
            "test_sender",
            {}
        )
        assert message.type == msg_type
        assert isinstance(message.serialize(), bytes)

def test_message_timestamp():
    """Test message timestamp handling."""
    message = Message.create(
        MessageType.HELLO,
        "test_sender",
        {}
    )
    
    assert message.timestamp is not None
    assert isinstance(message.timestamp, float)
    
    # Test timestamp in serialized message
    serialized = message.serialize()
    deserialized = Message.deserialize(serialized)
    assert deserialized.timestamp == message.timestamp

def test_message_data_types():
    """Test different data types in messages."""
    test_data = {
        "string": "test",
        "number": 123,
        "boolean": True,
        "list": [1, 2, 3],
        "dict": {"key": "value"},
        "null": None
    }
    
    message = Message.create(
        MessageType.HELLO,
        "test_sender",
        test_data
    )
    
    serialized = message.serialize()
    deserialized = Message.deserialize(serialized)
    
    assert deserialized.data == test_data

def test_large_message():
    """Test handling of large messages."""
    large_data = {
        "large_string": "x" * 1000000,  # 1MB string
        "large_list": list(range(10000))
    }
    
    message = Message.create(
        MessageType.HELLO,
        "test_sender",
        large_data
    )
    
    serialized = message.serialize()
    deserialized = Message.deserialize(serialized)
    
    assert deserialized.data == large_data 