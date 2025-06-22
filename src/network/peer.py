import asyncio
import socket
import hashlib
from typing import Optional, Dict, Set, Callable, Awaitable
from dataclasses import dataclass
from datetime import datetime
import logging
from .protocol import (
    Message, 
    MessageType, 
    ProtocolError, 
    CHUNK_SIZE, 
    CONNECTION_TIMEOUT, 
    READ_TIMEOUT,
    MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    MAX_MESSAGE_SIZE,
    MessageSizeError,
    InvalidMessageError
)
import uuid
import time
import struct

# Constants for timeouts and retries
CONNECTION_TIMEOUT = 10.0  # seconds
READ_TIMEOUT = 30.0  # seconds
WRITE_TIMEOUT = 30.0  # seconds
HEARTBEAT_INTERVAL = 30.0  # seconds
HEARTBEAT_TIMEOUT = 10.0  # seconds
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRIES = 3
CHUNK_SIZE = 8192  # 8KB chunks for reading/writing

@dataclass
class PeerInfo:
    """Information about a peer in the network."""
    id: str
    address: str
    port: int
    last_seen: datetime
    is_connected: bool = False

class Peer:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, peer_id: str):
        """Initialize a peer connection."""
        self.reader = reader
        self.writer = writer
        self.id = peer_id
        self.address = writer.get_extra_info('peername')[0]
        self.port = writer.get_extra_info('peername')[1]
        self.username = None
        self.last_seen = time.time()
        self.is_connected = False  # Only set to True after successful connect
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"[Peer __init__] Created Peer: id={self.id}, address={self.address}, port={self.port}, is_connected={self.is_connected}")
        self.is_disconnecting = False
        self.is_local = False
        self.known_peers = set()
        self._last_activity = time.time()
        self.retry_count = 0
        self.retry_delay = INITIAL_RETRY_DELAY
        self.max_retries = MAX_RETRIES
        self.connection_timeout = CONNECTION_TIMEOUT
        self.read_timeout = READ_TIMEOUT
        self.write_timeout = WRITE_TIMEOUT
        self.heartbeat_interval = HEARTBEAT_INTERVAL
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT
        self.heartbeat_task = None
        self.message_queue = asyncio.Queue()
        self.processing_task = None
        self._lock = asyncio.Lock()
        self.message_handlers: Dict[MessageType, Callable[[Message], Awaitable[None]]] = {}
        self.hello_sent = False
        self.hello_received = False

    @property
    def address(self) -> str:
        """Get peer address."""
        return self._address

    @address.setter
    def address(self, value: str):
        """Set peer address."""
        self._address = value

    @property
    def port(self) -> int:
        """Get peer port."""
        return self._port

    @port.setter
    def port(self, value: int):
        """Set peer port."""
        self._port = value

    @property
    def last_activity(self) -> float:
        """Get the last activity timestamp."""
        return self._last_activity

    @last_activity.setter
    def last_activity(self, value: float):
        """Set the last activity timestamp."""
        self._last_activity = value

    @property
    def last_seen(self) -> float:
        """Get the last seen timestamp (alias for last_activity)."""
        return self._last_activity

    @last_seen.setter
    def last_seen(self, value: float):
        """Set the last seen timestamp (alias for last_activity)."""
        self._last_activity = value

    def _generate_peer_id(self) -> str:
        """Generate a unique peer ID based on host and port."""
        data = f"{self.address}:{self.port}".encode()
        return hashlib.sha1(data).hexdigest()

    async def connect(self) -> bool:
        """Connect to the peer."""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting to connect to {self.address}:{self.port} (attempt {attempt + 1}/{self.max_retries})")
                # Verify connection was successful
                if not self.reader or not self.writer:
                    raise ConnectionError("Failed to establish connection")
                # Optionally, perform handshake or send a ping here
                self.is_connected = True
                self.logger.info(f"Connected to peer {self.address}:{self.port}")
                # Send HELLO message immediately after connecting, only once
                if not self.hello_sent:
                    hello_payload = {"username": self.username} if self.username else {}
                    hello_msg = Message.create(MessageType.HELLO, self.id, hello_payload)
                    await self.send_message(hello_msg)
                    self.hello_sent = True
                # Start heartbeat after sending HELLO
                if not self.heartbeat_task:
                    self.heartbeat_task = asyncio.create_task(self._heartbeat())
                return True
            except asyncio.TimeoutError:
                self.logger.error(f"Connection timeout to {self.address}:{self.port}")
            except ConnectionRefusedError:
                self.logger.error(f"Connection refused by {self.address}:{self.port}")
            except Exception as e:
                self.logger.error(f"Error connecting to {self.address}:{self.port}: {e}")
            # Wait before retrying
            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))
        self.is_connected = False
        self.logger.error(f"Failed to connect to peer {self.address}:{self.port} after {self.max_retries} attempts.")
        return False

    async def disconnect(self, send_goodbye: bool = True):
        """Disconnect from the peer."""
        if self.is_disconnecting:
            return
            
        self.is_disconnecting = True
        self.is_connected = False
        
        try:
            if send_goodbye and self.writer and not self.writer.is_closing():
                try:
                    # Create goodbye message with correct parameter name
                    goodbye_msg = Message(
                        type=MessageType.GOODBYE,
                        sender_id=self.id,
                        payload={'reason': 'disconnecting'}
                    )
                    await self.send_message(goodbye_msg)
                except Exception as e:
                    self.logger.debug(f"Error sending goodbye message: {e}")
                    
            if self.writer:
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception as e:
                    self.logger.debug(f"Error waiting for writer to close: {e}")
                    
            if self.reader:
                self.reader = None
                
            self.writer = None
            self.logger.info("Disconnected from peer")
            
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
        finally:
            self.is_disconnecting = False

    async def send_message(self, message: Message) -> bool:
        """Send a message to the peer."""
        self.logger.debug(f"[send_message] is_connected={self.is_connected}, writer={self.writer}, peer_id={self.id}")
        if not self.is_connected or not self.writer:
            self.logger.error(f"[send_message] Cannot send message: not connected (is_connected={self.is_connected}, writer={self.writer})")
            return False
            
        try:
            # Serialize message
            data = message.serialize()
            self.logger.debug(f"Serialized message size: {len(data)} bytes")

            # Send length prefix
            length_prefix = len(data).to_bytes(4, 'big')
            self.writer.write(length_prefix)
            await self.writer.drain()

            # Send message data
            self.writer.write(data)
            await self.writer.drain()

            self.logger.debug(f"Successfully sent message of size {len(data)} bytes")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return False

    async def request_file_chunk(self, file_id: str, chunk_index: int):
        """Requests a specific chunk of a file from the peer."""
        self.logger.debug(f"[request_file_chunk] is_connected={self.is_connected}, file_id={file_id}, chunk_index={chunk_index}, peer_id={self.id}")
        if not self.is_connected:
            self.logger.error("Cannot request chunk: not connected")
            return
            
        self.logger.debug(f"Requesting chunk {chunk_index} for file {file_id} from {self.id}")
        message = Message(
            type=MessageType.CHUNK_REQUEST,
            sender_id=self.id, # This should be our own ID
            payload={'file_id': file_id, 'chunk_index': chunk_index}
        )
        await self.send_message(message)

    async def send_file_chunk(self, file_id: str, chunk_index: int, chunk_data: bytes):
        """Sends a specific chunk of a file to the peer."""
        if not self.is_connected:
            self.logger.error("Cannot send chunk: not connected")
            return
            
        self.logger.debug(f"Sending chunk {chunk_index} for file {file_id} to {self.id}")
        message = Message(
            type=MessageType.CHUNK_RESPONSE,
            sender_id=self.id, # This should be our own ID
            payload={'file_id': file_id, 'chunk_index': chunk_index, 'data': chunk_data}
        )
        await self.send_message(message)

    async def read_message(self) -> Optional[Message]:
        """Read a message from the peer with detailed validation."""
        if not self.is_connected or self.reader is None:
            self.logger.debug("Cannot read message: not connected or reader is None")
            return None
            
        try:
            # Step 1: Read length prefix with longer timeout
            self.logger.debug("Step 1: Reading message length prefix")
            length_data = await asyncio.wait_for(
                self.reader.read(4),
                timeout=self.read_timeout  # Use longer timeout for normal operation
            )
            
            if not length_data:
                self.logger.debug("No data received when reading length prefix")
                return None
                
            if len(length_data) != 4:
                self.logger.error(f"Invalid length prefix size: {len(length_data)} bytes (expected 4)")
                return None
                
            # Step 2: Unpack length
            self.logger.debug("Step 2: Unpacking message length")
            try:
                message_length = struct.unpack('!I', length_data)[0]
                self.logger.debug(f"Unpacked message length: {message_length} bytes")
            except struct.error as e:
                self.logger.error(f"Failed to unpack message length: {e}")
                return None
                
            # Step 3: Validate length
            if message_length <= 0:
                self.logger.error(f"Invalid message length: {message_length} (must be positive)")
                return None
                
            if message_length > MAX_MESSAGE_SIZE:
                self.logger.error(f"Message length {message_length} exceeds maximum size {MAX_MESSAGE_SIZE}")
                return None
                
            # Step 4: Read message body
            self.logger.debug(f"Step 4: Reading message body of size {message_length}")
            message_data = await asyncio.wait_for(
                self.reader.read(message_length),
                timeout=self.read_timeout
            )
            
            if not message_data:
                self.logger.debug("No data received when reading message")
                return None
                
            if len(message_data) != message_length:
                self.logger.error(f"Message data length mismatch: got {len(message_data)}, expected {message_length}")
                return None
                
            # Step 5: Deserialize message
            self.logger.debug("Step 5: Deserializing message")
            try:
                message = Message.deserialize(message_data)
                self.logger.debug(f"Successfully deserialized message of type {message.type}")
                return message
            except Exception as e:
                self.logger.error(f"Failed to deserialize message: {e}")
                return None
                
        except asyncio.TimeoutError:
            self.logger.debug(f"Timeout reading message length after {self.read_timeout} seconds")
            return None
        except ConnectionResetError:
            self.logger.error("Connection reset while reading message")
            await self.disconnect()
            return None
        except Exception as e:
            self.logger.error(f"Error reading message: {e}")
            await self.disconnect()
            return None

    def register_handler(self, message_type: MessageType, handler: Callable[[Message], Awaitable[None]]):
        """Register a message handler for a specific message type."""
        self.message_handlers[message_type] = handler

    async def start_listening(self):
        """Start listening for incoming connections."""
        if not self.is_local:
            self.logger.debug("Not starting server for non-local peer")
            return

        try:
            # Create server socket
            server = await asyncio.start_server(
                self._handle_connection,
                self.address,
                self.port
            )
            
            self.logger.info(f"Started listening on {self.address}:{self.port}")
            
            # Keep the server running
            async with server:
                await server.serve_forever()
                
        except Exception as e:
            self.logger.error(f"Error starting server: {e}")
            raise

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle an incoming connection."""
        try:
            # Get peer address
            peer_addr = writer.get_extra_info('peername')
            self.logger.info(f"New connection from {peer_addr[0]}:{peer_addr[1]}")
            
            # Store reader and writer
            self.reader = reader
            self.writer = writer
            self.is_connected = True
            
            # Start message handling loop
            while self.is_connected:
                try:
                    message = await self.receive_message()
                    if message is None:
                        break
                        
                    # Handle message
                    if message.type in self.message_handlers:
                        await self.message_handlers[message.type](message)
                    else:
                        self.logger.warning(f"No handler for message type: {message.type}")
                        
                except Exception as e:
                    self.logger.error(f"Error handling message: {e}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Error handling connection: {e}")
        finally:
            await self.disconnect()

    async def ping(self) -> bool:
        """Send a PING message and wait for PONG response."""
        try:
            ping_msg = Message.create(
                MessageType.PING,
                self.id,
                {"timestamp": datetime.now().timestamp()}
            )
            await self.send_message(ping_msg)
            return True
        except Exception as e:
            self.logger.error(f"Error sending ping: {e}")
            return False 

    def get_known_peers(self) -> Set[PeerInfo]:
        """Get the set of peers known to this peer."""
        return self.known_peers

    def add_known_peer(self, peer_info: PeerInfo):
        """Add a peer to the known peers set."""
        self.known_peers.add(peer_info)

    def remove_known_peer(self, peer_id: str):
        """Remove a peer from the known peers set."""
        self.known_peers = {p for p in self.known_peers if p.id != peer_id} 

    async def _process_messages(self):
        """Process incoming messages."""
        while self.is_connected and not self.is_disconnecting:
            try:
                # Read message
                message = await self.read_message()
                if not message:
                    break
                    
                # Process message
                if message.type in self.message_handlers:
                    try:
                        await self.message_handlers[message.type](message)
                    except Exception as e:
                        self.logger.error(f"Error handling message: {e}")
                else:
                    self.logger.warning(f"No handler for message type: {message.type}")
                    
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
                break
                
        # Clean up if we exit the loop
        if self.is_connected:
            await self.disconnect()

    async def receive_message(self) -> Optional[Message]:
        """Receive a message from the peer."""
        return await self.read_message()

    async def _heartbeat(self):
        """Send periodic heartbeat messages to keep the connection alive."""
        try:
            while self.is_connected and not self.is_disconnecting:
                try:
                    # Check connection state
                    if not self.is_connected or not self.writer or self.writer.is_closing():
                        self.logger.debug("Connection no longer valid for heartbeat")
                        break
                        
                    # Send heartbeat message
                    heartbeat_msg = Message(
                        type=MessageType.HEARTBEAT,
                        sender_id=self.id,
                        payload={"timestamp": time.time()}
                    )
                    
                    if not await self.send_message(heartbeat_msg):
                        self.logger.error("Failed to send heartbeat")
                        break
                        
                    # Wait for next heartbeat
                    await asyncio.sleep(self.heartbeat_interval)
                    
                except Exception as e:
                    self.logger.error(f"Error sending heartbeat: {e}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Error in heartbeat loop: {e}")
        finally:
            if self.is_connected:
                await self.disconnect(send_goodbye=False)

    def register_message_handler(self, msg_type: MessageType, handler: Callable[[Message], Awaitable[None]]):
        """Register a handler for a specific message type."""
        self.message_handlers[msg_type] = handler 

    async def start(self):
        """Start the peer connection and message processing."""
        if self.is_connected:
            return True

        try:
            # If we have reader/writer, we're already connected
            if self.reader and self.writer:
                self.is_connected = True
            else:
                # Otherwise, establish connection
                success = await self.connect()
                if not success:
                    return False

            # Start message processing (integrated with main event loop)
            # Don't create separate tasks to avoid qasync conflicts
            
            # Start heartbeat if not local (integrated with main event loop)
            # Don't create separate tasks to avoid qasync conflicts
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting peer: {e}")
            return False

    async def close(self):
        """Close the peer connection."""
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
            self.is_connected = False
            self.logger.info(f"Closed connection to peer {self.id}")
        except Exception as e:
            self.logger.error(f"Error closing peer connection: {e}")

    def update_last_seen(self):
        """Update the last seen timestamp."""
        self.last_seen = time.time() 

    def register_default_handlers(self):
        self.register_message_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
        self.register_message_handler(MessageType.HELLO, self._handle_hello)

    async def _handle_heartbeat(self, message):
        self.logger.debug(f"Received HEARTBEAT from peer {self.id}")
        self.last_seen = time.time()

    async def _handle_hello(self, message):
        self.logger.info(f"Received HELLO from peer {self.id}")
        if not self.hello_received:
            self.hello_received = True
            # Only send HELLO back if we haven't sent one yet
            if not self.hello_sent:
                hello_msg = Message.create(MessageType.HELLO, self.id, {"username": self.username})
                await self.send_message(hello_msg)
                self.hello_sent = True
        # Start heartbeat after receiving HELLO
        if not self.heartbeat_task:
            self.heartbeat_task = asyncio.create_task(self._heartbeat()) 