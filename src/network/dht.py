import hashlib
import time
import asyncio
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging
from .protocol import Message, MessageType
from .peer import Peer, PeerInfo

class DHTError(Exception):
    """Base exception class for DHT-related errors."""
    pass

@dataclass
class KBucket:
    nodes: List[PeerInfo]
    last_updated: datetime = field(default_factory=datetime.now)
    k: int = 20  # Maximum number of nodes per bucket

    def __post_init__(self):
        if not self.nodes:
            self.nodes = []

    def add_node(self, node: PeerInfo) -> bool:
        """Add a node to the bucket if there's space or if it's closer than existing nodes."""
        if len(self.nodes) < self.k:
            self.nodes.append(node)
            self.last_updated = datetime.now()
            return True
        return False

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the bucket."""
        for i, node in enumerate(self.nodes):
            if node.id == node_id:
                self.nodes.pop(i)
                return True
        return False

    def update_node(self, node: PeerInfo) -> bool:
        """Update an existing node's information."""
        for i, existing_node in enumerate(self.nodes):
            if existing_node.id == node.id:
                self.nodes[i] = node
                self.last_updated = datetime.now()
                return True
        return False

class DHT:
    """Distributed Hash Table for peer discovery and routing."""
    
    def __init__(self, host: str, port: int, bootstrap_nodes: List[Tuple[str, int]] = None):
        self.host = host
        self.port = port
        self.bootstrap_nodes = bootstrap_nodes or []
        self.peers: Dict[str, Peer] = {}
        self.logger = logging.getLogger(__name__)
        self._server = None
        self._running = False
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        
    @property
    def node_id(self) -> str:
        """Get the node ID (host:port)."""
        return f"{self.host}:{self.port}"
        
    def get_connected_peers(self) -> List[Peer]:
        """Get a list of currently connected peers."""
        return list(self.peers.values())
        
    async def start(self):
        """Start the DHT network."""
        self.logger.info("Starting DHT network")
        try:
            # Start listening for incoming connections
            self._server = await asyncio.start_server(
                self._handle_connection,
                self.host,
                self.port
            )
            self.logger.info(f"Local peer started listening on {self.host}:{self.port}")
            
            # Register message handlers
            self._register_message_handlers()
            
            # Start periodic cleanup task
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
            
            # Connect to bootstrap nodes if available
            if self.bootstrap_nodes:
                await self._connect_to_bootstrap_nodes()
            else:
                self.logger.info("No bootstrap nodes configured, starting as first node")
            
            self._running = True
            self.logger.info("DHT network started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start DHT network: {e}")
            raise
            
    async def _periodic_cleanup(self):
        """Periodically clean up stale peers."""
        while self._running:
            try:
                current_time = time.time()
                stale_peers = []
                
                # Find stale peers
                for peer_id, peer in self.peers.items():
                    if current_time - peer.last_seen > 300:  # 5 minutes
                        stale_peers.append(peer_id)
                
                # Remove stale peers
                for peer_id in stale_peers:
                    await self.remove_peer(peer_id)
                    
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")
                await asyncio.sleep(60)  # Wait a minute before retrying
                
    async def stop(self):
        """Stop the DHT network."""
        if self._running:
            self._running = False
            
            # Cancel cleanup task
            if self._cleanup_task:
                self._cleanup_task.cancel()
                try:
                    await self._cleanup_task
                except asyncio.CancelledError:
                    pass
                    
            # Disconnect from all peers
            for peer_id in list(self.peers.keys()):
                await self.remove_peer(peer_id)
                
            # Close server
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                
            self.logger.info("DHT network stopped")
            
    async def remove_peer(self, peer_id: str):
        """Remove a peer from the network."""
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            await peer.disconnect()
            del self.peers[peer_id]
            self.logger.info(f"Removed peer {peer_id}")
            
    def _register_message_handlers(self):
        """Register message handlers for the DHT."""
        # Get the local peer instance
        local_peer = self.get_local_peer()
        if local_peer:
            # Register handlers for different message types
            local_peer.register_message_handler(MessageType.PEER_LIST, self.handle_peer_list)
            local_peer.register_message_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
            local_peer.register_message_handler(MessageType.GOODBYE, self._handle_goodbye)
            
    async def _handle_heartbeat(self, message: Message, peer: Peer):
        """Handle heartbeat messages from peers."""
        try:
            # Update peer's last seen timestamp
            peer.last_seen = time.time()
            self.logger.debug(f"Received heartbeat from {peer.peer_id}")
            
            # Send acknowledgment if needed
            await peer.send_message(Message(
                type=MessageType.HEARTBEAT,
                sender_id=self.node_id,
                payload={'status': 'ok'}
            ))
            
        except Exception as e:
            self.logger.error(f"Error handling heartbeat: {e}")
            
    async def _handle_goodbye(self, message: Message, peer: Peer):
        """Handle goodbye messages from peers."""
        try:
            reason = message.payload.get('reason', 'unknown')
            self.logger.info(f"Peer {peer.peer_id} is disconnecting. Reason: {reason}")
            await self.remove_peer(peer.peer_id)
            
        except Exception as e:
            self.logger.error(f"Error handling goodbye message: {e}")
            
    def get_local_peer(self) -> Optional[Peer]:
        """Get the local peer instance."""
        return self.peers.get(self.node_id)
        
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle new incoming connections."""
        try:
            # Get peer address
            peer_addr = writer.get_extra_info('peername')
            self.logger.info(f"New connection from {peer_addr[0]}:{peer_addr[1]}")
            
            # Create new peer instance
            peer = Peer(
                peer_id=f"{peer_addr[0]}:{peer_addr[1]}",
                host=peer_addr[0],
                port=peer_addr[1],
                reader=reader,
                writer=writer
            )
            
            # Register message handlers for this peer
            peer.register_message_handler(MessageType.PEER_LIST, self.handle_peer_list)
            peer.register_message_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
            peer.register_message_handler(MessageType.GOODBYE, self._handle_goodbye)
            
            # Add to peers list
            self.peers[peer.peer_id] = peer
            
            # Start message processing
            await peer.start()
            
            # Send our username to the peer
            await peer.send_message(Message(
                type=MessageType.USER_INFO,
                sender_id=self.node_id,
                payload={'username': self.username}
            ))
            
        except Exception as e:
            self.logger.error(f"Error handling connection: {e}")
            if writer:
                writer.close()
                await writer.wait_closed()
                
    async def handle_peer_list(self, message: Message, peer: Peer):
        """Handle peer list messages."""
        try:
            peers = message.payload.get('peers', [])
            self.logger.debug(f"Received peer list with {len(peers)} peers from {peer.peer_id}")
            
            # Process each peer in the list
            for peer_info in peers:
                try:
                    # Skip if it's our own peer info
                    if peer_info['id'] == self.node_id:
                        continue
                        
                    # Skip if we already know this peer
                    if peer_info['id'] in self.peers:
                        continue
                        
                    # Connect to the new peer
                    await self.connect_to_peer(peer_info['host'], peer_info['port'])
                    
                except Exception as e:
                    self.logger.error(f"Error processing peer {peer_info.get('id')}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error handling peer list: {e}")
            
    async def connect_to_peer(self, host: str, port: int) -> Optional[Peer]:
        """Connect to a peer."""
        peer_id = f"{host}:{port}"
        
        # Skip if already connected
        if peer_id in self.peers:
            return self.peers[peer_id]
            
        try:
            # Attempt connection
            reader, writer = await asyncio.open_connection(host, port)
            
            # Create peer instance
            peer = Peer(
                peer_id=peer_id,
                host=host,
                port=port,
                reader=reader,
                writer=writer
            )
            
            # Register message handlers
            peer.register_message_handler(MessageType.PEER_LIST, self.handle_peer_list)
            peer.register_message_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
            peer.register_message_handler(MessageType.GOODBYE, self._handle_goodbye)
            peer.register_message_handler(MessageType.USER_INFO, self._handle_user_info)
            
            # Add to peers list
            self.peers[peer_id] = peer
            
            # Start message processing
            await peer.start()
            
            # Send our username to the peer
            await peer.send_message(Message(
                type=MessageType.USER_INFO,
                sender_id=self.node_id,
                payload={'username': self.username}
            ))
            
            self.logger.info(f"Connected to peer {peer_id}")
            return peer
            
        except Exception as e:
            self.logger.error(f"Error connecting to peer {peer_id}: {e}")
            return None

    async def _handle_user_info(self, message: Message, peer: Peer):
        """Handle user info messages from peers."""
        try:
            username = message.payload.get('username')
            if username:
                # Update peer's username in database
                await self.db_manager.update_peer_username(peer.peer_id, username)
                self.logger.info(f"Updated username for peer {peer.peer_id}: {username}")
        except Exception as e:
            self.logger.error(f"Error handling user info: {e}")

    def _get_bucket_index(self, node_id: str) -> int:
        """Get the index of the k-bucket for a given node ID."""
        # XOR the node IDs and find the first differing bit
        xor = int(self.node_id, 16) ^ int(node_id, 16)
        if xor == 0:
            return 0
        return 159 - (xor.bit_length() - 1)

    def _distance(self, id1: str, id2: str) -> int:
        """Calculate XOR distance between two node IDs."""
        return int(id1, 16) ^ int(id2, 16)

    def add_node(self, node: PeerInfo) -> bool:
        """Add a node to the appropriate k-bucket."""
        bucket_index = self._get_bucket_index(node.id)
        bucket = self.k_buckets[bucket_index]
        
        # Check if node already exists
        for existing_node in bucket.nodes:
            if existing_node.id == node.id:
                return bucket.update_node(node)
        
        # Try to add the node
        if bucket.add_node(node):
            return True
            
        # If bucket is full, we need to check if we should split it
        if len(bucket.nodes) == self.k:
            # TODO: Implement bucket splitting logic
            pass
            
        return False

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the DHT."""
        bucket_index = self._get_bucket_index(node_id)
        return self.k_buckets[bucket_index].remove_node(node_id)

    def find_node(self, target_id: str) -> List[PeerInfo]:
        """Find the k closest nodes to the target ID."""
        bucket_index = self._get_bucket_index(target_id)
        closest_nodes = []
        
        # Start with the target's bucket
        closest_nodes.extend(self.k_buckets[bucket_index].nodes)
        
        # If we don't have enough nodes, look in adjacent buckets
        left = bucket_index - 1
        right = bucket_index + 1
        
        while len(closest_nodes) < self.k and (left >= 0 or right < 160):
            if left >= 0:
                closest_nodes.extend(self.k_buckets[left].nodes)
            if right < 160:
                closest_nodes.extend(self.k_buckets[right].nodes)
            left -= 1
            right += 1
        
        # Sort by distance to target
        closest_nodes.sort(key=lambda x: self._distance(x.id, target_id))
        return closest_nodes[:self.k]

    async def store(self, key: str, value: str) -> bool:
        """Store a key-value pair in the DHT."""
        # Find the k closest nodes to the key
        target_nodes = self.find_node(key)
        
        # Send STORE requests to the closest nodes
        success = False
        for node in target_nodes:
            if node.id in self.peers:
                peer = self.peers[node.id]
                try:
                    store_msg = Message.create(
                        MessageType.STORE,
                        self.node_id,
                        {"key": key, "value": value}
                    )
                    await peer.send_message(store_msg)
                    success = True
                except Exception as e:
                    self.logger.error(f"Error storing value: {e}")
        
        return success

    async def find_value(self, key: str) -> Optional[str]:
        """Find a value in the DHT."""
        # Find the k closest nodes to the key
        target_nodes = self.find_node(key)
        
        # Send FIND_VALUE requests to the closest nodes
        for node in target_nodes:
            if node.id in self.peers:
                peer = self.peers[node.id]
                try:
                    find_msg = Message.create(
                        MessageType.FIND_VALUE,
                        self.node_id,
                        {"key": key}
                    )
                    await peer.send_message(find_msg)
                    # TODO: Handle response
                except Exception as e:
                    self.logger.error(f"Error finding value: {e}")
        
        return None

    async def join_network(self, bootstrap_nodes: List[Tuple[str, int]]):
        """Join the DHT network using bootstrap nodes."""
        for host, port in bootstrap_nodes:
            try:
                peer = Peer(host, port)
                if await peer.connect():
                    self.peers[peer.peer_id] = peer
                    # Send FIND_NODE request to bootstrap node
                    find_msg = Message.create(
                        MessageType.FIND_NODE,
                        self.node_id,
                        {"target": self.node_id}
                    )
                    await peer.send_message(find_msg)
            except Exception as e:
                self.logger.error(f"Error joining network: {e}")

    def cleanup(self):
        """Clean up old nodes and merge buckets if needed."""
        current_time = datetime.now()
        for bucket in self.k_buckets:
            # Remove nodes that haven't been seen in a while
            bucket.nodes = [
                node for node in bucket.nodes
                if (current_time - node.last_seen).total_seconds() < 3600  # 1 hour
            ]
            # TODO: Implement bucket merging logic

    def split_bucket(self, bucket_index: int) -> None:
        """Split a bucket when it becomes too full."""
        bucket = self.k_buckets[bucket_index]
        if len(bucket.nodes) < self.k:
            return

        # Create new bucket
        new_bucket = set()
        old_bucket = set()

        # Split based on the next bit in the ID
        split_bit = bucket_index + 1
        for peer in bucket.nodes:
            if self._get_bit(peer.id, split_bit):
                new_bucket.add(peer)
            else:
                old_bucket.add(peer)

        # Update buckets
        bucket.nodes = list(old_bucket)
        self.k_buckets.insert(bucket_index + 1, KBucket(list(new_bucket)))

        # Update routing table
        self._update_routing_table()
        self.logger.debug(f"Split bucket {bucket_index} into two buckets")

    def merge_buckets(self, bucket_index: int) -> None:
        """Merge a bucket with its neighbor if both are under-utilized."""
        if bucket_index >= len(self.k_buckets) - 1:
            return

        current_bucket = self.k_buckets[bucket_index]
        next_bucket = self.k_buckets[bucket_index + 1]

        # Check if both buckets are under-utilized
        if len(current_bucket.nodes) + len(next_bucket.nodes) <= self.k:
            # Merge buckets
            merged_nodes = current_bucket.nodes + next_bucket.nodes
            self.k_buckets[bucket_index] = KBucket(merged_nodes)
            self.k_buckets.pop(bucket_index + 1)

            # Update routing table
            self._update_routing_table()
            self.logger.debug(f"Merged buckets {bucket_index} and {bucket_index + 1}")

    async def handle_response(self, response: Message) -> None:
        """Handle a response message from a peer."""
        try:
            if response.type == MessageType.PONG:
                # Update peer's last seen time
                peer_id = response.sender
                if peer_id in self.peers:
                    self.peers[peer_id].last_seen = datetime.now()
                    self.logger.debug(f"Updated last seen time for peer {peer_id}")

            elif response.type == MessageType.PEER_LIST:
                # Add new peers from the response
                peers = response.data.get("peers", [])
                for peer_info in peers:
                    await self.add_peer(PeerInfo(**peer_info))
                self.logger.debug(f"Added {len(peers)} peers from response")

            elif response.type == MessageType.FILE_LIST:
                # Update file list from the response
                files = response.data.get("files", [])
                for file_info in files:
                    self.file_manager.add_remote_file(file_info)
                self.logger.debug(f"Updated file list with {len(files)} files")

            elif response.type == MessageType.FILE_RESPONSE:
                # Handle file data response
                file_id = response.data.get("file_id")
                chunk_index = response.data.get("chunk_index")
                chunk_data = response.data.get("chunk_data")
                if all([file_id, chunk_index is not None, chunk_data]):
                    await self.file_manager.handle_file_chunk(file_id, chunk_index, chunk_data)
                    self.logger.debug(f"Received chunk {chunk_index} for file {file_id}")

            else:
                self.logger.warning(f"Unhandled response type: {response.type}")

        except Exception as e:
            self.logger.error(f"Error handling response: {e}")
            raise DHTError(f"Failed to handle response: {e}")

    def _update_routing_table(self) -> None:
        """Update the routing table after bucket changes."""
        self.routing_table.clear()
        for bucket in self.k_buckets:
            for peer in bucket.nodes:
                self.routing_table[peer.id] = peer
        self.logger.debug("Updated routing table")

    async def broadcast_peer_list(self):
        """Broadcast the current peer list to all connected peers."""
        if not self.peers:
            return
            
        # Get list of connected peers
        connected_peers = [p for p in self.peers.values() if p.is_connected]
        if not connected_peers:
            return
            
        # Create peer list message with minimal data
        peer_list = []
        for peer in connected_peers:
            peer_list.append({
                'peer_id': peer.peer_id,
                'host': peer.host,
                'port': peer.port
            })
            
        # Split peer list into smaller chunks if needed
        chunk_size = 50  # Maximum peers per message
        for i in range(0, len(peer_list), chunk_size):
            chunk = peer_list[i:i + chunk_size]
            message = Message(
                type=MessageType.PEER_LIST,
                sender_id=self.local_peer.peer_id,
                payload={
                    'peers': chunk,
                    'chunk_index': i // chunk_size,
                    'total_chunks': (len(peer_list) + chunk_size - 1) // chunk_size
                }
            )
            
            # Send to each connected peer
            for peer in connected_peers:
                try:
                    if peer.is_connected:
                        await peer.send_message(message)
                except Exception as e:
                    self.logger.error(f"Error sending peer list to {peer.host}:{peer.port}: {e}")

    async def handle_peer_list(self, message: Message):
        """Handle incoming peer list message."""
        try:
            peers_data = message.payload.get('peers', [])
            chunk_index = message.payload.get('chunk_index', 0)
            total_chunks = message.payload.get('total_chunks', 1)
            
            # Store chunk in temporary storage
            if not hasattr(self, '_peer_list_chunks'):
                self._peer_list_chunks = {}
            self._peer_list_chunks[chunk_index] = peers_data
            
            # If we have all chunks, process them
            if len(self._peer_list_chunks) == total_chunks:
                # Combine all chunks
                all_peers = []
                for i in range(total_chunks):
                    all_peers.extend(self._peer_list_chunks[i])
                    
                # Clear temporary storage
                self._peer_list_chunks = {}
                
                # Process combined peer list
                for peer_data in all_peers:
                    peer_id = peer_data.get('peer_id')
                    host = peer_data.get('host')
                    port = peer_data.get('port')
                    
                    if not all([peer_id, host, port]):
                        continue
                        
                    # Skip if it's our own peer info
                    if peer_id == self.local_peer.peer_id:
                        continue
                        
                    # Skip if we already know this peer
                    if peer_id in self.peers:
                        continue
                        
                    # Try to connect to the new peer
                    try:
                        await self.connect_to_peer(host, port)
                    except Exception as e:
                        self.logger.error(f"Error connecting to peer {host}:{port}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error handling peer list: {e}")