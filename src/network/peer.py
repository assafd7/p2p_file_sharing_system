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
    def __init__(self, host: str, port: int, peer_id: str = None, is_local: bool = False, 
                 reader: Optional[asyncio.StreamReader] = None, writer: Optional[asyncio.StreamWriter] = None):
        """Initialize a peer connection."""
        self.host = host
        self.port = port
        self.peer_id = peer_id or str(uuid.uuid4())
        self.id = self.peer_id  # Add id property for compatibility
        self.address = self.host  # Add address property for compatibility
        self.reader = reader
        self.writer = writer
        self.is_connected = bool(reader and writer)  # Set connected if reader/writer provided
        self.is_disconnecting = False  # Add flag to track disconnection state
        self.is_local = is_local  # Add back is_local flag
        self.known_peers = set()  # Track known peers
        self.logger = logging.getLogger(f"Peer-{self.peer_id}")
        self.last_activity = time.time()
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

    def _generate_peer_id(self) -> str:
        """Generate a unique peer ID based on host and port."""
        data = f"{self.host}:{self.port}".encode()
        return hashlib.sha1(data).hexdigest()

    async def connect(self) -> bool:
        """Connect to the peer."""
        if self.is_connected:
            return True
            
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting to connect to {self.host}:{self.port} (attempt {attempt + 1}/{self.max_retries})")
                
                # Create connection with timeouts
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.connection_timeout
                )
                
                # Verify connection was successful
                if not self.reader or not self.writer:
                    raise ConnectionError("Failed to establish connection")
                    
                self.is_connected = True
                self.logger.info(f"Connected to peer {self.host}:{self.port}")
                
                # Start message processing
                self.processing_task = asyncio.create_task(self._process_messages())
                
                # Start heartbeat if not local
                if not self.is_local:
                    self.heartbeat_task = asyncio.create_task(self._heartbeat())
                    
                return True
                
            except asyncio.TimeoutError:
                self.logger.error(f"Connection timeout to {self.host}:{self.port}")
            except ConnectionRefusedError:
                self.logger.error(f"Connection refused by {self.host}:{self.port}")
            except Exception as e:
                self.logger.error(f"Error connecting to {self.host}:{self.port}: {e}")
                
            # Wait before retrying
            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))
                
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
                        sender_id=self.peer_id,
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
        if not self.is_connected or not self.writer:
            self.logger.error("Cannot send message: not connected")
            return False
            
        try:
            # Log message before sending
            self.logger.debug(f"Sending message type {message.type} to {self.host}:{self.port}")
            
            # Serialize message
            try:
                data = message.serialize()
                self.logger.debug(f"Serialized message size: {len(data)} bytes")
            except Exception as e:
                self.logger.error(f"Error serializing message: {e}")
                return False
            
            # Send data in chunks
            chunk_size = 8192  # 8KB chunks
            total_sent = 0
            
            while total_sent < len(data):
                chunk = data[total_sent:total_sent + chunk_size]
                try:
                    self.writer.write(chunk)
                    await self.writer.drain()
                    total_sent += len(chunk)
                    self.logger.debug(f"Sent {total_sent}/{len(data)} bytes")
                except Exception as e:
                    self.logger.error(f"Error sending chunk: {e}")
                    return False
            
            self.logger.debug(f"Successfully sent message of size {len(data)} bytes")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return False

    async def read_message(self) -> Optional[Message]:
        """Read a message from the peer with detailed validation."""
        if not self.is_connected:
            self.logger.debug("Cannot read message: not connected")
            return None
            
        try:
            # Step 1: Read length prefix
            self.logger.debug("Step 1: Reading message length prefix")
            length_data = await asyncio.wait_for(
                self.reader.read(4),
                timeout=self.read_timeout
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
                
            # Step 4: Read message data
            self.logger.debug(f"Step 4: Reading message data ({message_length} bytes)")
            try:
                message_data = await asyncio.wait_for(
                    self.reader.read(message_length),
                    timeout=self.read_timeout
                )
            except asyncio.TimeoutError:
                self.logger.error(f"Timeout reading message data after {self.read_timeout} seconds")
                return None
                
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
            self.logger.error(f"Timeout reading message length after {self.read_timeout} seconds")
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
                self.host,
                self.port
            )
            
            self.logger.info(f"Started listening on {self.host}:{self.port}")
            
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
                self.peer_id,
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
                        sender_id=self.peer_id,
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

            # Start message processing
            self.processing_task = asyncio.create_task(self._process_messages())
            
            # Start heartbeat if not local
            if not self.is_local:
                self.heartbeat_task = asyncio.create_task(self._heartbeat())
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting peer: {e}")
            return False 