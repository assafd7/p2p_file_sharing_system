from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QStatusBar, QMessageBox,
    QFileDialog, QProgressBar, QMenu, QSystemTrayIcon,
    QTabWidget, QTreeWidget, QTreeWidgetItem, QListWidgetItem, QInputDialog, QLineEdit
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QThread
from PyQt6.QtGui import QIcon, QAction
import logging
from typing import Dict, List, Optional
from datetime import datetime
import os
from pathlib import Path
import asyncio

from src.file_management.file_manager import FileManager
from src.network.dht import DHT
from src.database.db_manager import DatabaseManager

class TransferWorker(QThread):
    progress_updated = pyqtSignal(str, float, str)  # transfer_id, progress, status
    transfer_completed = pyqtSignal(str, bool)  # transfer_id, success

    def __init__(self, transfer_id: str, file_manager, parent=None):
        super().__init__(parent)
        self.transfer_id = transfer_id
        self.file_manager = file_manager
        self.is_running = True

    def run(self):
        while self.is_running:
            status = self.file_manager.get_transfer_status(self.transfer_id)
            if status:
                progress, status_text = status
                self.progress_updated.emit(self.transfer_id, progress, status_text)
                if progress >= 1.0 or status_text.startswith("failed"):
                    self.is_running = False
                    self.transfer_completed.emit(self.transfer_id, progress >= 1.0)
            self.msleep(100)  # Update every 100ms

    def stop(self):
        self.is_running = False

class MainWindow(QMainWindow):
    """Main window of the P2P file sharing application."""
    
    def __init__(self, file_manager: FileManager, network_manager: DHT,
                 db_manager: DatabaseManager, user_id: str, username: str):
        """Initialize the main window."""
        super().__init__()
        self.file_manager = file_manager
        self.network_manager = network_manager
        self.db_manager = db_manager
        self.user_id = user_id
        self.username = username
        self.logger = logging.getLogger(__name__)
        self.transfer_workers: Dict[str, TransferWorker] = {}

        # Set window properties
        self.setWindowTitle("P2P File Sharing System")
        self.setGeometry(100, 100, 800, 600)

        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Create user info bar
        user_bar = QHBoxLayout()
        user_label = QLabel(f"Logged in as: {username}")
        user_label.setStyleSheet("font-weight: bold;")
        user_bar.addWidget(user_label)
        user_bar.addStretch()
        
        logout_button = QPushButton("Logout")
        logout_button.clicked.connect(self.handle_logout)
        user_bar.addWidget(logout_button)
        
        main_layout.addLayout(user_bar)

        # Create tab widget
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)

        # Setup tabs
        self.tab_widget.addTab(self.setup_files_tab(), "Files")
        self.tab_widget.addTab(self.setup_transfers_tab(), "Transfers")
        self.tab_widget.addTab(self.setup_peers_tab(), "Peers")
        self.tab_widget.addTab(self.setup_settings_tab(), "Settings")

        # Create status bar
        self.statusBar().showMessage("Ready")

        # Setup menu bar
        self.setup_menu_bar()

        # Setup update timer
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_ui)
        self.update_timer.start(1000)  # Update every second
        
        # Initial UI update
        QTimer.singleShot(0, self.update_ui)

    def setup_files_tab(self):
        """Setup the files tab with file list and controls."""
        files_tab = QWidget()
        layout = QVBoxLayout(files_tab)

        # Create file list
        self.file_list = QTreeWidget()
        self.file_list.setHeaderLabels(["Name", "Size", "Owner", "Status"])
        self.file_list.setColumnWidth(0, 300)  # Name column
        self.file_list.setColumnWidth(1, 100)  # Size column
        self.file_list.setColumnWidth(2, 150)  # Owner column
        self.file_list.setColumnWidth(3, 100)  # Status column
        self.file_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.file_list.customContextMenuRequested.connect(self.show_context_menu)
        layout.addWidget(self.file_list)

        # Create file controls
        controls_layout = QHBoxLayout()
        
        self.share_button = QPushButton("Share File")
        self.share_button.clicked.connect(self.share_file)
        controls_layout.addWidget(self.share_button)

        self.download_button = QPushButton("Download")
        self.download_button.clicked.connect(self.download_file)
        controls_layout.addWidget(self.download_button)

        self.delete_button = QPushButton("Delete")
        self.delete_button.clicked.connect(self.delete_file)
        layout.addLayout(controls_layout)
        return files_tab

    def setup_transfers_tab(self):
        """Setup the transfers tab with transfer list and progress bars."""
        transfers_tab = QWidget()
        layout = QVBoxLayout(transfers_tab)

        # Create transfer list
        self.transfer_list = QTreeWidget()
        self.transfer_list.setHeaderLabels(["File", "Progress", "Status", "Speed"])
        layout.addWidget(self.transfer_list)

        # Create transfer controls
        controls_layout = QHBoxLayout()
        
        self.pause_button = QPushButton("Pause")
        self.pause_button.clicked.connect(self.pause_transfer)
        controls_layout.addWidget(self.pause_button)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_transfer)
        controls_layout.addWidget(self.cancel_button)

        layout.addLayout(controls_layout)
        return transfers_tab

    def setup_peers_tab(self):
        """Set up the peers tab."""
        peers_tab = QWidget()
        layout = QVBoxLayout()
        
        # Create peer list
        self.peer_list = QTreeWidget()
        self.peer_list.setHeaderLabels(["Username", "Address", "Port", "Status"])
        self.peer_list.setColumnCount(4)
        layout.addWidget(self.peer_list)
        
        # Create buttons
        button_layout = QHBoxLayout()
        
        # Connect button
        connect_btn = QPushButton("Connect to Peer")
        connect_btn.clicked.connect(self.connect_to_peer)
        button_layout.addWidget(connect_btn)
        
        # Disconnect button
        disconnect_btn = QPushButton("Disconnect")
        disconnect_btn.clicked.connect(self.disconnect_from_peer)
        button_layout.addWidget(disconnect_btn)
        
        # Refresh button
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.update_peer_list)
        button_layout.addWidget(refresh_btn)
        
        layout.addLayout(button_layout)
        peers_tab.setLayout(layout)
        return peers_tab

    def setup_settings_tab(self):
        """Setup the settings tab with configuration options."""
        settings_tab = QWidget()
        layout = QVBoxLayout(settings_tab)

        # Network settings
        network_group = QWidget()
        network_layout = QVBoxLayout(network_group)
        
        port_layout = QHBoxLayout()
        port_layout.addWidget(QLabel("Port:"))
        self.port_input = QLineEdit()
        self.port_input.setText("8000")
        port_layout.addWidget(self.port_input)
        network_layout.addLayout(port_layout)

        # Storage settings
        storage_group = QWidget()
        storage_layout = QVBoxLayout(storage_group)
        
        storage_path_layout = QHBoxLayout()
        storage_path_layout.addWidget(QLabel("Storage Path:"))
        self.storage_path_input = QLineEdit()
        self.storage_path_input.setReadOnly(True)
        storage_path_layout.addWidget(self.storage_path_input)
        browse_button = QPushButton("Browse")
        browse_button.clicked.connect(self.browse_storage_path)
        storage_path_layout.addWidget(browse_button)
        storage_layout.addLayout(storage_path_layout)

        # Add groups to main layout
        layout.addWidget(network_group)
        layout.addWidget(storage_group)
        layout.addStretch()

        return settings_tab

    def setup_menu_bar(self):
        """Setup the menu bar with actions."""
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("File")
        
        share_action = QAction("Share File", self)
        share_action.triggered.connect(self.share_file)
        file_menu.addAction(share_action)
        
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # View menu
        view_menu = menubar.addMenu("View")
        
        refresh_action = QAction("Refresh", self)
        refresh_action.triggered.connect(self.update_ui)
        view_menu.addAction(refresh_action)

        # Help menu
        help_menu = menubar.addMenu("Help")
        
        about_action = QAction("About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def show_context_menu(self, position):
        """Show context menu for file list."""
        item = self.file_list.itemAt(position)
        if not item:
            return

        # Get file info before creating the menu
        file_info = item.data(0, Qt.ItemDataRole.UserRole)
        if not file_info:
            return

        # Select the item when right-clicking
        self.file_list.setCurrentItem(item)

        menu = QMenu()
        download_action = menu.addAction("Download")
        delete_action = menu.addAction("Delete")

        action = menu.exec(self.file_list.mapToGlobal(position))
        if action == download_action:
            self.download_file()
        elif action == delete_action:
            # Pass the file info directly instead of the item
            self.delete_file(file_info=file_info)

    def share_file(self):
        """Share a file with the network."""
        try:
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Select File to Share",
                "",
                "All Files (*.*)"
            )
            
            if not file_path:
                return
                
            try:
                # Add file to shared files
                metadata = self.file_manager.add_file(file_path, self.user_id)
                
                if not metadata:
                    raise Exception("No metadata returned from file manager")
                    
                self.logger.debug(f"File shared successfully: {file_path}")
                self.logger.debug(f"File metadata: {metadata}")
                
                # Verify file appears in shared files list
                async def verify_and_update():
                    shared_files = await self.file_manager.get_shared_files_async()
                    if not any(f.hash == metadata.hash for f in shared_files):
                        raise Exception("File not found in shared files list after sharing")
                    await self.update_file_list()
                
                # Run verification in event loop
                loop = asyncio.get_event_loop()
                loop.create_task(verify_and_update())
                
                self.show_info(f"File shared: {file_path}")
                
            except Exception as e:
                self.logger.error(f"Error sharing file: {str(e)}")
                self.show_error(f"Failed to share file: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in share_file: {str(e)}")
            self.show_error(f"Error in share_file: {str(e)}")

    def download_file(self):
        """Download a selected file."""
        selected_items = self.file_list.selectedItems()
        if not selected_items:
            self.show_warning("Please select a file to download")
            return

        file_info = selected_items[0].data(0, Qt.ItemDataRole.UserRole)
        if not file_info:
            self.show_error("Could not get file information")
            return

        try:
            save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save File",
                file_info.name,
                "All Files (*.*)"
            )
            if save_path:
                self.file_manager.download_file(file_info.hash, save_path)
                self.show_info(f"Downloading: {file_info.name}")
                self.update_transfer_list()
        except Exception as e:
            self.show_error(f"Error downloading file: {e}")

    def delete_file(self, item=None, file_info=None):
        """Delete a selected file.
        
        Args:
            item: Optional QTreeWidgetItem to delete. If None, uses the currently selected item.
            file_info: Optional FileMetadata object. If provided, uses this instead of getting from item.
        """
        # If file_info is not provided, get it from the selected item
        if file_info is None:
            selected_items = [item] if item else self.file_list.selectedItems()
            if not selected_items:
                self.show_warning("Please select a file to delete")
                return
            file_info = selected_items[0].data(0, Qt.ItemDataRole.UserRole)
            if not file_info:
                self.show_error("Could not get file information")
                return

        # Confirm deletion with user
        reply = QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Are you sure you want to delete {file_info.name}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.file_manager.delete_file(file_info.hash, self.user_id)
                self.show_info(f"File deleted: {file_info.name}")
                
                # Update file list asynchronously
                loop = asyncio.get_event_loop()
                loop.create_task(self.update_file_list())
                
            except Exception as e:
                self.show_error(f"Error deleting file: {e}")

    def pause_transfer(self):
        """Pause a selected transfer."""
        selected_items = self.transfer_list.selectedItems()
        if not selected_items:
            self.show_warning("Please select a transfer to pause")
            return

        transfer_info = selected_items[0].data(Qt.ItemDataRole.UserRole)
        try:
            self.file_manager.pause_transfer(transfer_info.id)
            self.show_info(f"Transfer paused: {transfer_info.file_name}")
            self.update_transfer_list()
        except Exception as e:
            self.show_error(f"Error pausing transfer: {e}")

    def cancel_transfer(self):
        """Cancel a selected transfer."""
        selected_items = self.transfer_list.selectedItems()
        if not selected_items:
            self.show_warning("Please select a transfer to cancel")
            return

        transfer_info = selected_items[0].data(Qt.ItemDataRole.UserRole)
        reply = QMessageBox.question(
            self,
            "Confirm Cancellation",
            f"Are you sure you want to cancel {transfer_info.file_name}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.file_manager.cancel_transfer(transfer_info.id)
                self.show_info(f"Transfer cancelled: {transfer_info.file_name}")
                self.update_transfer_list()
            except Exception as e:
                self.show_error(f"Error cancelling transfer: {e}")

    def connect_to_peer(self):
        """Connect to a peer using the specified address."""
        try:
            # Get peer address from user
            address, ok = QInputDialog.getText(
                self, "Connect to Peer", "Enter peer address (host:port):"
            )
            
            if not ok or not address:
                return
                
            # Parse address
            try:
                host, port_str = address.split(":")
                port = int(port_str)
            except ValueError:
                self.show_error("Invalid address format. Please use host:port")
                return
                
            # Attempt connection
            async def connect():
                success = await self.network_manager.connect_to_peer(host, port)
                if success:
                    self.show_info(f"Successfully connected to {address}")
                    self.update_peer_list()  # Refresh peer list
                else:
                    self.show_error(f"Failed to connect to {address}")
            
            # Run connection in event loop
            loop = asyncio.get_event_loop()
            loop.create_task(connect())
            
        except Exception as e:
            self.logger.error(f"Error connecting to peer: {e}")
            self.show_error(f"Error connecting to peer: {str(e)}")

    def disconnect_from_peer(self):
        """Disconnect from a selected peer."""
        selected_items = self.peer_list.selectedItems()
        if not selected_items:
            self.show_warning("Please select a peer to disconnect")
            return

        peer_info = selected_items[0].data(Qt.ItemDataRole.UserRole)
        try:
            self.network_manager.disconnect_from_peer(peer_info.id)
            self.show_info(f"Disconnected from peer: {peer_info.address}")
            self.update_peer_list()
        except Exception as e:
            self.show_error(f"Error disconnecting from peer: {e}")

    def browse_storage_path(self):
        """Open dialog to select storage path."""
        path = QFileDialog.getExistingDirectory(
            self,
            "Select Storage Directory",
            self.storage_path_input.text()
        )
        if path:
            self.storage_path_input.setText(path)

    def show_about(self):
        """Show about dialog."""
        QMessageBox.about(
            self,
            "About P2P File Sharing System",
            "A secure and efficient peer-to-peer file sharing system.\n\n"
            "Version 1.0.0\n"
            "© 2024 Your Name"
        )

    def update_ui(self):
        """Update the UI with current state."""
        try:
            # Update file list
            self.update_file_list()
            
            # Update transfer list
            self.update_transfer_list()
            
            # Update peer list
            self.update_peer_list()
                
        except Exception as e:
            self.logger.error(f"Error updating UI: {e}")

    def update_file_list(self):
        """Update the file list in the UI."""
        try:
            self.file_list.clear()
            files = self.file_manager.get_shared_files_sync()
            
            for file in files:
                item = QTreeWidgetItem([
                    file['name'],
                    str(file['size']),
                    file['type'],
                    file['status']
                ])
                self.file_list.addTopLevelItem(item)
                
            # Resize columns to content
            for i in range(self.file_list.columnCount()):
                self.file_list.resizeColumnToContents(i)
                
        except Exception as e:
            self.logger.error(f"Error updating file list: {e}")

    def update_transfer_list(self):
        """Update the transfer list display."""
        self.transfer_list.clear()
        transfers = self.file_manager.get_active_transfers()
        for transfer_info in transfers:
            item = QListWidgetItem(transfer_info.file_name)
            item.setData(Qt.ItemDataRole.UserRole, transfer_info)
            self.transfer_list.addItem(item)

    def update_peer_list(self):
        """Update the peer list display."""
        try:
            self.logger.info("Starting peer list update")
            self.peer_list.clear()
            
            # Get connected peers
            peers = self.network_manager.get_connected_peers()
            self.logger.info(f"Found {len(peers)} connected peers")
            
            for peer in peers:
                try:
                    # Get peer info from database
                    peer_info = self.db_manager.get_peer_sync(peer.id)
                    self.logger.info(f"Retrieved peer info for {peer.id}: {peer_info}")
                    
                    if peer_info:
                        username = peer_info.get('username', 'Unknown')
                        self.logger.info(f"Using username from database: {username}")
                    else:
                        username = peer.username if hasattr(peer, 'username') else 'Unknown'
                        self.logger.info(f"Using username from peer object: {username}")
                    
                    # Add peer to list
                    item = QTreeWidgetItem([
                        username,
                        peer.address,
                        str(peer.port),
                        "Connected"
                    ])
                    self.peer_list.addTopLevelItem(item)
                    self.logger.info(f"Added peer to list: {username} ({peer.id})")
                except Exception as e:
                    self.logger.error(f"Error adding peer {peer.id} to list: {e}")
            
            # Resize columns to fit content
            for i in range(self.peer_list.columnCount()):
                self.peer_list.resizeColumnToContents(i)
            self.logger.info("Completed peer list update")
        except Exception as e:
            self.logger.error(f"Error updating peer list: {e}")

    def on_peer_connected(self, peer):
        """Handle peer connection."""
        self.logger.info(f"Peer connected: {peer.id}")
        # Schedule UI update in the main thread
        QTimer.singleShot(0, self.update_peer_list)

    def on_peer_disconnected(self, peer):
        """Handle peer disconnection."""
        self.logger.info(f"Peer disconnected: {peer.id}")
        # Schedule UI update in the main thread
        QTimer.singleShot(0, self.update_peer_list)

    def on_peer_updated(self, peer):
        """Handle peer update."""
        self.logger.info(f"Peer updated: {peer.id}")
        # Schedule UI update in the main thread
        QTimer.singleShot(0, self.update_peer_list)

    def cleanup(self):
        """Clean up resources before closing."""
        try:
            # Stop all transfers
            self.file_manager.cancel_all_transfers()
            
            # Disconnect from all peers
            self.network_manager.disconnect_all_peers()
            
            # Close database connection
            self.db_manager.close()
            
            # Save settings
            self.save_settings()
            
            self.show_info("Cleanup completed")
        except Exception as e:
            self.show_error(f"Error during cleanup: {e}")

    def closeEvent(self, event):
        """Handle window close event."""
        self.cleanup()
        event.accept() 

    def show_error(self, message):
        QMessageBox.critical(self, "Error", str(message))

    def show_info(self, message):
        QMessageBox.information(self, "Information", str(message))

    def show_warning(self, message):
        QMessageBox.warning(self, "Warning", str(message))

    def handle_logout(self):
        """Handle logout button click."""
        reply = QMessageBox.question(
            self,
            "Logout",
            "Are you sure you want to logout?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.close()
            # The main application will handle showing the auth window again

    def save_settings(self):
        """Save application settings."""
        # Implementation of save_settings method
        pass

    def save_settings(self):
        """Save application settings."""
        # Implementation of save_settings method
        pass 