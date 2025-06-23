import hashlib
import time
import asyncio
from typing import Dict, List, Set, Optional, Tuple, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
import logging
from .protocol import Message, MessageType
from .peer import Peer, PeerInfo
from src.database.db_manager import DatabaseManager
from src.file_management.file_metadata import FileMetadata
from PyQt6.QtCore import QTimer

class DHTError(Exception):
    """Base exception class for DHT-related errors."""
    pass

@dataclass
class KBucket:
    nodes: List[PeerInfo] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)
    k: int = 20

    def add_node(self, node: PeerInfo) -> bool:
        if not any(n.id == node.id for n in self.nodes):
            if len(self.nodes) < self.k:
                self.nodes.append(node)
                self.last_updated = time.time()
                return True
        return False

    def remove_node(self, node_id: str) -> bool:
        initial_len = len(self.nodes)
        self.nodes = [n for n in self.nodes if n.id != node_id]
        return len(self.nodes) < initial_len
        
    def update_node(self, node: PeerInfo):
        for i, existing_node in enumerate(self.nodes):
            if existing_node.id == node.id:
                self.nodes[i].last_seen = datetime.now()
                self.last_updated = time.time()
                return

class DHT:
    """
    Distributed Hash Table (DHT) for peer discovery and routing.
    
    This class implements a basic DHT with k-buckets, bucket splitting, merging, and an eviction policy.
    All peer selection, routing, and lookup logic now uses the DHT bucket structure.
    
    DHT Completion Plan (all steps implemented):
      1. Add missing DHT utility methods (e.g., _get_bit)
      2. Prepare and test bucket splitting/merging logic (debug methods)
      3. Integrate bucket splitting into add_node
      4. Implement basic eviction policy
      5. Update find_node to use buckets efficiently
      6. Update routing and lookup logic to use DHT buckets
      7. Final cleanup and documentation (this step)
    
    See test_dht.py for tests covering all steps. Future DHT changes should be tested similarly.
    """
    
    def __init__(self, host: str, port: int, bootstrap_nodes: Optional[List[Tuple[str, int]]] = None, 
                 username: str = "Anonymous", db_manager: Optional[DatabaseManager] = None, file_manager=None):
        """Initialize the DHT network."""
        self.host = host
        self.port = port
        self._node_id = f"{self.host}:{self.port}"
        self.bootstrap_nodes = bootstrap_nodes or []
        self.username = username
        self.db_manager = db_manager
        self.peers: Dict[str, Peer] = {}
        self.logger = logging.getLogger(__name__)
        self._server: Optional[asyncio.AbstractServer] = None
        self._running = False
        self._connection_semaphore = asyncio.Semaphore(10)
        self._peer_tasks: Dict[str, asyncio.Task] = {}
        
        self.k = 20
        self.k_buckets: List[KBucket] = [KBucket() for _ in range(160)] # 160 buckets for SHA-1 hash

        self.on_peer_connected: Optional[Callable[[Peer], Awaitable[None]]] = None
        self.on_peer_disconnected: Optional[Callable[[Peer], Awaitable[None]]] = None
        self.on_file_metadata_received: Optional[Callable[[FileMetadata, Peer], Awaitable[None]]] = None
        self.on_chunk_request: Optional[Callable[[str, int, Peer], Awaitable[None]]] = None
        self.on_chunk_response: Optional[Callable[[str, int, bytes, Peer], Awaitable[None]]] = None

        self.message_handlers = {
            MessageType.PEER_LIST: self._handle_peer_list_v1,
            MessageType.GOODBYE: self._handle_goodbye,
            MessageType.FILE_METADATA: self._handle_file_metadata,
            MessageType.CHUNK_REQUEST: self._handle_chunk_request,
            MessageType.CHUNK_RESPONSE: self._handle_chunk_response,
            MessageType.FIND_NODE: self._handle_find_node,
            MessageType.HELLO: self._handle_hello,
        }
        self.file_manager = file_manager
        self.routing_table = {}
        self._seen_metadata = set()
        
    @property
    def node_id(self) -> str:
        """Get the node ID (host:port)."""
        return self._node_id
        
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
            
            # Start periodic cleanup as a background task (but don't create a separate task)
            # We'll handle cleanup in the main event loop instead
            
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
            
    async def _connect_to_bootstrap_nodes(self):
        """Connect to bootstrap nodes."""
        self.logger.info(f"Connecting to bootstrap nodes: {self.bootstrap_nodes}")
        for host, port in self.bootstrap_nodes:
            try:
                peer = await self.connect_to_peer(host, port)
                if peer:
                    self.logger.info(f"Successfully connected to bootstrap node {host}:{port}")
                else:
                    self.logger.warning(f"Failed to connect to bootstrap node {host}:{port} (connection returned None)")
            except Exception as e:
                self.logger.error(f"Failed to connect to bootstrap node {host}:{port}: {e}")
            
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
            
            # Disconnect from all peers
            for peer_id in list(self.peers.keys()):
                await self.remove_peer(peer_id)
                
            # Close server
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                
            # Cancel all peer message handling tasks
            for task in list(self._peer_tasks.values()):
                task.cancel()
            await asyncio.gather(*self._peer_tasks.values(), return_exceptions=True)
            self._peer_tasks.clear()
            
            self.logger.info("DHT network stopped")
            
    async def remove_peer(self, peer_id: str):
        """Remove a peer from the network."""
        if peer_id in self._peer_tasks:
            self._peer_tasks.pop(peer_id).cancel()
        if peer_id in self.peers:
            peer = self.peers.pop(peer_id)
            if self.on_peer_disconnected: await self.on_peer_disconnected(peer)
            await peer.disconnect()
            self.remove_node(peer_id)
            self.logger.info(f"Removed peer {peer_id}")
            
    def _register_message_handlers(self):
        """Register message handlers for the DHT."""
        self.message_handlers = {
            MessageType.PEER_LIST: self._handle_peer_list_v1,
            MessageType.GOODBYE: self._handle_goodbye,
            MessageType.FILE_METADATA: self._handle_file_metadata,
            MessageType.CHUNK_REQUEST: self._handle_chunk_request,
            MessageType.CHUNK_RESPONSE: self._handle_chunk_response,
            MessageType.FIND_NODE: self._handle_find_node,
            MessageType.HELLO: self._handle_hello,
        }
        self.logger.debug("Registered message handlers")
            
    async def _handle_peer_list_v1(self, message: Message, peer: Peer):
        for p_info in message.payload.get('peers', []):
            if p_info.get('id') != self.node_id:
                await self.connect_to_peer(p_info['host'], p_info['port'])

    async def _handle_goodbye(self, message: Message, peer: Peer):
        """Handle goodbye message."""
        try:
            self.logger.info(f"Received goodbye from peer {peer.id}")
            await self.remove_peer(peer.id)
        except Exception as e:
            self.logger.error(f"Error handling goodbye: {e}")

    def get_local_peer(self) -> Optional[Peer]:
        """Get the local peer instance."""
        return self.peers.get(self.node_id)
        
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle new peer connection."""
        peer_address, peer_port = writer.get_extra_info('peername')
        self.logger.info(f"[DHT] New incoming connection from {peer_address}:{peer_port}")
        peer_id = f"{peer_address}:{peer_port}"

        if peer_id == self.node_id:
            self.logger.info(f"[DHT] Incoming connection from self ({peer_id}), closing.")
            writer.close(); await writer.wait_closed(); return

        async with self._connection_semaphore:
            if peer_id in self.peers:
                self.logger.info(f"[DHT] Peer {peer_id} already connected, closing new connection.")
                writer.close(); await writer.wait_closed(); return

            peer = Peer(reader, writer, peer_id)
            self.peers[peer_id] = peer
            self.logger.info(f"[DHT] Added peer {peer_id} to self.peers. Total peers: {len(self.peers)}")
            self.add_node(PeerInfo(id=peer.id, address=peer.address, port=peer.port, last_seen=datetime.now()))
            
            if self.on_peer_connected: await self.on_peer_connected(peer)
            self.schedule_peer_message_task(peer)
            try:
                await writer.wait_closed()
                self.logger.info(f"[DHT] Connection to {peer_id} closed.")
            except Exception as e:
                self.logger.error(f"[DHT] Exception while waiting for connection to close: {e}")
            
    def _start_peer_message_loop(self, peer: Peer):
        """Creates and tracks the message handling task for a peer."""
        if peer.id in self._peer_tasks:
            return # Task already running
        def schedule_task():
            task = asyncio.create_task(self._handle_peer_messages_loop(peer))
            self._peer_tasks[peer.id] = task
            task.add_done_callback(lambda t: self._peer_tasks.pop(peer.id, None))
        QTimer.singleShot(0, schedule_task)

    async def connect_to_peer(self, host: str, port: int) -> Optional[Peer]:
        """Connect to a peer without starting background tasks (qasync-safe)."""
        peer_id = f"{host}:{port}"
        if peer_id == self.node_id or peer_id in self.peers:
            return self.peers.get(peer_id)

        try:
            async with self._connection_semaphore:
                reader, writer = await asyncio.open_connection(host, port)
                peer = Peer(reader, writer, peer_id)
                if not await peer.connect():
                    self.logger.error(f"Peer.connect() failed for {peer_id}")
                    return None
                self.peers[peer_id] = peer
                self.add_node(PeerInfo(id=peer.id, address=peer.address, port=peer.port, last_seen=datetime.now()))
                
                if self.on_peer_connected: await self.on_peer_connected(peer)
                self.schedule_peer_message_task(peer)
                # Do NOT start peer message loop here!
                return peer
        except Exception as e:
            self.logger.error(f"Failed to connect to {peer_id}: {e}")
            return None

    def schedule_peer_message_task(self, peer: Peer):
        """Schedule a peer message handling task to start after the current slot returns (qasync-safe)."""
        try:
            peer_id = peer.id
            # Only schedule if not already present
            if peer_id in self._peer_tasks:
                existing_task = self._peer_tasks[peer_id]
                if not existing_task.done():
                    self.logger.debug(f"Peer task for {peer_id} already exists and running")
                    return
            def schedule_task():
                loop = asyncio.get_event_loop()
                task = loop.create_task(self._handle_peer_messages_loop(peer))
                self._peer_tasks[peer_id] = task
                task.add_done_callback(lambda t: self._peer_tasks.pop(peer_id, None))
            self.logger.debug(f"Scheduled peer message task for {peer_id}")
            QTimer.singleShot(0, schedule_task)
        except Exception as e:
            self.logger.error(f"Error scheduling peer message task for {peer.id}: {e}")

    def schedule_metadata_broadcast(self, metadata: FileMetadata):
        """
        Safely schedules the broadcast of file metadata to run on the next
        iteration of the event loop, avoiding qasync conflicts.
        """
        self.logger.info(f"Scheduled broadcast for file: {metadata.name}")
        def schedule_task():
            loop = asyncio.get_running_loop()
            loop.create_task(self.broadcast_file_metadata(metadata))
        QTimer.singleShot(0, schedule_task)

    def _cleanup_peer_task(self, peer_id: str, task: asyncio.Task):
        """Clean up peer task when it's done."""
        try:
            if not task.cancelled():
                task.result()  # This will raise any exception that occurred
        except Exception as e:
            self.logger.error(f"Peer task for {peer_id} failed: {e}")
        finally:
            # Remove task from tracking
            if peer_id in self._peer_tasks:
                del self._peer_tasks[peer_id]
            self.logger.debug(f"Cleaned up peer task for {peer_id}")

    async def _handle_peer_messages_loop(self, peer: Peer):
        try:
            while self._running:
                message = await peer.read_message()
                if message is None: break
                
                peer.last_seen = time.time()

                if message.type is not None:
                    handler = self.message_handlers.get(message.type)
                    if handler:
                        await handler(message, peer)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
            self.logger.info(f"Connection lost or cancelled with {peer.id}")
        except Exception as e:
            self.logger.error(f"Error handling messages from {peer.id}: {e}", exc_info=True)
        finally:
            await self.remove_peer(peer.id)

    def _get_bucket_index(self, node_id: str) -> int:
        """Get the index of the k-bucket for a given node ID."""
        distance = self._distance(self.node_id, node_id)
        return distance.bit_length() - 1 if distance != 0 else 0

    def _distance(self, id1: str, id2: str) -> int:
        """Calculate XOR distance between two node IDs."""
        h1 = int(hashlib.sha1(id1.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(id2.encode()).hexdigest(), 16)
        return h1 ^ h2

    def add_node(self, node: PeerInfo):
        """
        Add a node to the appropriate k-bucket.
        Step 3 of DHT completion plan: Integrate bucket splitting when a bucket is full.
        """
        bucket_index = self._get_bucket_index(node.id)
        bucket = self.k_buckets[bucket_index]
        
        # Check if node already exists
        for existing_node in bucket.nodes:
            if existing_node.id == node.id:
                return bucket.update_node(node)
        
        # Try to add the node
        if bucket.add_node(node):
            return True
        
        # If bucket is full, split it and try again
        if len(bucket.nodes) == self.k:
            self.logger.info(f"[DHT] Bucket {bucket_index} full, splitting (Step 3)")
            self.split_bucket(bucket_index)
            # After splitting, recalculate the bucket index (it may have changed)
            bucket_index = self._get_bucket_index(node.id)
            bucket = self.k_buckets[bucket_index]
            if bucket.add_node(node):
                return True
            # TODO: Step 4 - If still full after splitting, implement eviction policy here
            # Step 4 of DHT completion plan: Evict least recently seen node if still full
            if len(bucket.nodes) == self.k:
                # Find the node with the oldest last_seen
                oldest_index = 0
                oldest_time = None
                for i, n in enumerate(bucket.nodes):
                    if oldest_time is None or n.last_seen < oldest_time:
                        oldest_time = n.last_seen
                        oldest_index = i
                evicted_node = bucket.nodes.pop(oldest_index)
                self.logger.info(f"[DHT] Evicted node {evicted_node.id} from bucket {bucket_index} to add new node {node.id} (Step 4)")
                bucket.add_node(node)
                return True
        return False

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the DHT."""
        bucket_index = self._get_bucket_index(node_id)
        return self.k_buckets[bucket_index].remove_node(node_id)

    def find_node(self, target_id: str) -> List[PeerInfo]:
        """
        Step 5 of DHT completion plan: Efficiently find the K closest nodes to a given ID using the bucket structure.
        """
        bucket_index = self._get_bucket_index(target_id)
        candidates = list(self.k_buckets[bucket_index].nodes)
        left = bucket_index - 1
        right = bucket_index + 1
        # Expand to neighboring buckets until we have at least k candidates
        while len(candidates) < self.k and (left >= 0 or right < len(self.k_buckets)):
            if left >= 0:
                candidates.extend(self.k_buckets[left].nodes)
                left -= 1
            if len(candidates) >= self.k:
                break
            if right < len(self.k_buckets):
                candidates.extend(self.k_buckets[right].nodes)
                right += 1
        # Sort by XOR distance to target_id
        candidates.sort(key=lambda p: self._distance(p.id, target_id))
        return candidates[:self.k]

    async def store(self, key: str, value: str) -> bool:
        """
        Store a key-value pair in the DHT.
        Step 6 of DHT completion plan: Use find_node for peer selection.
        """
        # Find the k closest nodes to the key using the bucket structure
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
        """
        Find a value in the DHT.
        Step 6 of DHT completion plan: Use find_node for peer selection.
        """
        # Find the k closest nodes to the key using the bucket structure
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
                peer = await self.connect_to_peer(host, port)
                if peer:
                    self.peers[peer.id] = peer
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
        new_bucket = []
        old_bucket = []

        # Split based on the next bit in the ID
        split_bit = bucket_index + 1
        if split_bit >= 160 or split_bit < 0:
            self.logger.warning(f"split_bucket: split_bit {split_bit} out of range, skipping split.")
            return
        for peer in bucket.nodes:
            if self._get_bit(peer.id, split_bit):
                new_bucket.append(peer)
            else:
                old_bucket.append(peer)

        # Update buckets
        bucket.nodes = old_bucket
        self.k_buckets.insert(bucket_index + 1, KBucket(new_bucket))

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
                peer_id = response.sender_id
                if peer_id in self.peers:
                    self.peers[peer_id].last_seen = time.time()
                    self.logger.debug(f"Updated last seen time for peer {peer_id}")

            elif response.type == MessageType.FILE_LIST:
                # Update file list from the response
                files = response.payload.get("files", [])
                if self.file_manager is not None:
                    for file_info in files:
                        self.file_manager.add_remote_file(file_info)
                    self.logger.debug(f"Updated file list with {len(files)} files")
                else:
                    self.logger.warning("No file_manager set; cannot update file list.")

            elif response.type == MessageType.FILE_RESPONSE:
                # Handle file data response
                file_id = response.payload.get("file_id")
                chunk_index = response.payload.get("chunk_index")
                chunk_data = response.payload.get("chunk_data")
                if all([file_id, chunk_index is not None, chunk_data]):
                    if self.file_manager is not None:
                        await self.file_manager.handle_file_chunk(file_id, chunk_index, chunk_data)
                        self.logger.debug(f"Received chunk {chunk_index} for file {file_id}")
                    else:
                        self.logger.warning("No file_manager set; cannot handle file chunk.")

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
                'peer_id': peer.id,
                'host': peer.address,
                'port': peer.port
            })
            
        # Split peer list into smaller chunks if needed
        chunk_size = 50  # Maximum peers per message
        for i in range(0, len(peer_list), chunk_size):
            chunk = peer_list[i:i + chunk_size]
            message = Message(
                type=MessageType.PEER_LIST,
                sender_id=self.node_id,
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
                    self.logger.error(f"Error sending peer list to {peer.address}:{peer.port}: {e}")

    async def _handle_file_metadata(self, message: Message, peer: Peer):
        """Handle incoming file metadata"""
        self.logger.debug(f"Handling file metadata from peer {peer.address}:{peer.port}")
        try:
            metadata_dict = message.payload
            self.logger.debug(f"Received metadata for file: {metadata_dict.get('name')}")
            self.logger.debug(f"Full metadata: {metadata_dict}")
            
            # Convert dictionary to FileMetadata object
            metadata = FileMetadata.from_dict(metadata_dict)
            self.logger.debug(f"Converted metadata to FileMetadata object: {metadata}")
            
            # Check if we've seen this metadata before
            metadata_id = f"{metadata.hash}_{metadata.owner_id}"
            if metadata_id in self._seen_metadata:
                self.logger.debug(f"Already seen metadata for file {metadata.name}, skipping")
                return
            
            self.logger.debug(f"Marking metadata as seen: {metadata_id}")
            self._seen_metadata.add(metadata_id)
            
            # Store metadata in database
            self.logger.debug("Storing metadata in database")
            if self.db_manager is not None:
                await self.db_manager.store_file_metadata(metadata)
                self.logger.debug("Successfully stored metadata in database")
            else:
                self.logger.warning("No db_manager set; cannot store file metadata.")
            
            # Notify UI if callback exists
            if hasattr(self, 'on_file_metadata_received') and self.on_file_metadata_received is not None:
                self.logger.debug("Notifying UI about new file metadata")
                await self.on_file_metadata_received(metadata, peer)
                self.logger.debug("UI notification complete")
            
            # Forward metadata to other peers if TTL > 0
            if metadata.ttl > 0:
                self.logger.debug(f"Forwarding metadata (TTL: {metadata.ttl})")
                metadata.ttl -= 1
                await self.broadcast_file_metadata(metadata)
                self.logger.debug("Metadata forwarding complete")
            
        except Exception as e:
            self.logger.error(f"Error handling file metadata: {str(e)}", exc_info=True)

    async def _handle_file_metadata_request(self, message: Message, peer: Peer) -> None:
        """Handle file metadata request message."""
        try:
            self.logger.info(f"Received file metadata request from peer {peer.id}")
            
            # Get requested file ID
            file_id = message.payload.get('file_id')
            if not file_id:
                self.logger.error("File metadata request missing file_id")
                return
                
            # Get metadata
            metadata = await self.get_metadata(file_id)
            if not metadata:
                self.logger.debug(f"File metadata not found for {file_id}")
                return
                
            # Send response
            response = Message(
                type=MessageType.FILE_METADATA_RESPONSE,
                sender_id=self.node_id,
                payload=metadata.to_dict()
            )
            await peer.send_message(response)
            self.logger.debug(f"Sent file metadata response to peer {peer.id}")
            
        except Exception as e:
            self.logger.error(f"Error handling file metadata request: {e}")
            raise

    async def _handle_file_metadata_response(self, message: Message, peer: Peer) -> None:
        """Handle file metadata response message."""
        try:
            self.logger.info(f"Received file metadata response from peer {peer.id}")
            
            # Parse metadata
            metadata = FileMetadata.from_dict(message.payload)
            
            # Store metadata
            await self.add_metadata(metadata)
            
            # Notify UI if callback exists
            if hasattr(self, 'on_file_metadata_received') and self.on_file_metadata_received is not None:
                self.on_file_metadata_received(metadata, peer)
                
        except Exception as e:
            self.logger.error(f"Error handling file metadata response: {e}")
            raise

    async def has_seen_metadata(self, metadata: FileMetadata) -> bool:
        """Check if we've seen this metadata before."""
        metadata_id = f"{metadata.file_id}_{metadata.owner_id}"
        return metadata_id in self._seen_metadata

    async def mark_metadata_seen(self, metadata: FileMetadata, peer_id: str) -> None:
        """Mark metadata as seen from a specific peer."""
        metadata_id = f"{metadata.file_id}_{metadata.owner_id}"
        self._seen_metadata.add(metadata_id)
        self.logger.debug(f"Marked metadata as seen: {metadata_id} from peer {peer_id}")

    async def add_metadata(self, metadata: FileMetadata) -> None:
        """Add metadata to storage."""
        try:
            if self.db_manager:
                # Store in database
                await self.db_manager.store_file_metadata(metadata)
                self.logger.debug(f"Stored metadata in database: {metadata.name}")
            else:
                self.logger.warning("No database manager available for storing metadata")
        except Exception as e:
            self.logger.error(f"Error storing metadata: {e}")
            raise

    async def get_metadata(self, file_id: str) -> Optional[FileMetadata]:
        """Get metadata from storage."""
        try:
            if self.db_manager:
                # Get from database
                metadata = await self.db_manager.get_file_metadata(file_id)
                if metadata:
                    self.logger.debug(f"Retrieved metadata from database: {metadata.name}")
                return metadata
            else:
                self.logger.warning("No database manager available for retrieving metadata")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving metadata: {e}")
            return None

    async def _handle_chunk_request(self, message: Message, peer: Peer) -> None:
        """Handle chunk request message."""
        file_id = message.payload.get('file_id')
        chunk_index = message.payload.get('chunk_index')
        # This needs to be handled by FileManager
        self.logger.warning("Chunk request handling needs to be delegated to FileManager")

    async def _handle_chunk_response(self, message: Message, peer: Peer) -> None:
        """Handle chunk response message."""
        file_id = message.payload.get('file_id')
        chunk_index = message.payload.get('chunk_index')
        # This needs to be handled by FileManager
        self.logger.warning("Chunk response handling needs to be delegated to FileManager")

    async def _handle_find_node(self, message: Message, peer: Peer):
        target_id = message.payload.get('target_id')
        if not target_id: return

        closest_nodes = self.find_node(target_id)
        response_payload = {
            'nodes': [{'id': n.id, 'address': n.address, 'port': n.port} for n in closest_nodes]
        }
        response = Message(type=MessageType.PEER_LIST, payload=response_payload)
        await peer.send_message(response)

    async def _handle_hello(self, message, peer):
        self.logger.info(f"Received HELLO from {peer.id}")
        # Set the peer's username if present in the payload
        if 'username' in message.payload:
            peer.username = message.payload['username']
        # Do not send a HELLO back; handshake is handled by Peer logic

    async def broadcast_file_metadata(self, metadata: FileMetadata):
        """Broadcast file metadata to all connected peers."""
        for peer in self.peers.values():
            if peer.is_connected:
                try:
                    message = Message(
                        type=MessageType.FILE_METADATA,
                        sender_id=self.node_id,
                        payload=metadata.to_dict()
                    )
                    await peer.send_message(message)
                except Exception as e:
                    self.logger.error(f"Error broadcasting file metadata to {peer.id}: {e}")

    def _get_bit(self, node_id: str, bit_index: int) -> int:
        """
        Step 1 of DHT completion plan: Utility for bucket splitting.
        Get the value (0 or 1) of the bit at position `bit_index` in the SHA-1 hash of node_id.
        """
        if bit_index < 0 or bit_index >= 160:
            return 0
        h = int(hashlib.sha1(node_id.encode()).hexdigest(), 16)
        return (h >> (160 - bit_index - 1)) & 1

    # Step 2: Prepare and test bucket splitting/merging logic (non-disruptive)
    # The following debug methods are for manual testing only and should not be called from production code.
    def debug_split_bucket(self, bucket_index: int):
        """
        Step 2 of DHT completion plan: Manually trigger a split of the specified bucket for testing.
        This is for debug/manual use only.
        """
        if 0 <= bucket_index < len(self.k_buckets):
            self.logger.info(f"[DEBUG] Splitting bucket {bucket_index}")
            self.split_bucket(bucket_index)
            self.logger.info(f"[DEBUG] Bucket {bucket_index} split complete. Total buckets: {len(self.k_buckets)}")
        else:
            self.logger.warning(f"[DEBUG] Invalid bucket index: {bucket_index}")

    def debug_merge_buckets(self, bucket_index: int):
        """
        Step 2 of DHT completion plan: Manually trigger a merge of the specified bucket with its neighbor for testing.
        This is for debug/manual use only.
        """
        if 0 <= bucket_index < len(self.k_buckets) - 1:
            self.logger.info(f"[DEBUG] Merging bucket {bucket_index} with {bucket_index + 1}")
            self.merge_buckets(bucket_index)
            self.logger.info(f"[DEBUG] Buckets {bucket_index} and {bucket_index + 1} merged. Total buckets: {len(self.k_buckets)}")
        else:
            self.logger.warning(f"[DEBUG] Invalid bucket index for merge: {bucket_index}")

    # TODO: Step 3 will integrate bucket splitting/merging into add_node and main workflow.