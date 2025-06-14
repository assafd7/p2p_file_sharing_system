import sys
import os
import asyncio
import socket
from pathlib import Path
from PyQt6.QtWidgets import QApplication, QMessageBox
from PyQt6.QtCore import QTimer

from src.ui.main_window import MainWindow
from src.ui.auth_window import AuthWindow
from src.network.dht import DHT
from src.network.security import SecurityManager
from src.file_management.file_manager import FileManager
from src.database.db_manager import DatabaseManager
from src.utils.logging_config import setup_logging, get_logger
from src.config import (
    DEFAULT_HOST, DEFAULT_PORT, BOOTSTRAP_NODES,
    TEMP_DIR, FILES_DIR, CACHE_DIR, DB_PATH,
    WINDOW_TITLE, WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT
)

class P2PFileSharingApp:
    def __init__(self):
        """Initialize the P2P file sharing application."""
        self.logger = get_logger("main")
        self.logger.info("Initializing P2P file sharing application")
        
        # Set up logging
        setup_logging()
        
        # Create Qt application first
        self.app = QApplication(sys.argv)
        self.app.setApplicationName(WINDOW_TITLE)
        
        try:
            # Create data directories
            self.create_data_directories()
            
            # Initialize components
            self.initialize_components()
            
            # Create auth window
            self.auth_window = AuthWindow(self.db_manager, self.security_manager)
            self.auth_window.auth_successful.connect(self.on_auth_successful)
            self.auth_window.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
            
            # Set up asyncio event loop
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # Set up timer for processing events
            self.timer = QTimer()
            self.timer.timeout.connect(self.process_events)
            self.timer.start(100)  # Process events every 100ms
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {e}")
            QMessageBox.critical(None, "Initialization Error", 
                               f"Failed to initialize application: {str(e)}")
            sys.exit(1)
    
    def create_data_directories(self):
        """Create necessary data directories."""
        self.logger.debug("Creating data directories")
        
        for directory in [TEMP_DIR, FILES_DIR, CACHE_DIR]:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Created directory: {directory}")
                # Verify directory exists and is writable
                if not directory.exists():
                    raise RuntimeError(f"Failed to create directory: {directory}")
                if not os.access(directory, os.W_OK):
                    raise RuntimeError(f"Directory not writable: {directory}")
                self.logger.debug(f"Directory {directory} is ready for use")
            except Exception as e:
                self.logger.error(f"Error creating directory {directory}: {e}")
                raise
    
    def initialize_components(self):
        """Initialize application components."""
        self.logger.info("Initializing application components")
        
        try:
            # Initialize security manager
            self.security_manager = SecurityManager()
            self.logger.debug("Security manager initialized")
            
            # Initialize database manager
            self.db_manager = DatabaseManager(DB_PATH)
            self.logger.debug("Database manager initialized")
            
            # Initialize file manager
            self.file_manager = FileManager(
                storage_dir=FILES_DIR,
                temp_dir=TEMP_DIR,
                cache_dir=CACHE_DIR,
                db_manager=self.db_manager
            )
            self.logger.debug("File manager initialized")
            
            # Get local IP address
            local_ip = self.get_local_ip()
            self.logger.info(f"Local IP address: {local_ip}")
            
            # Initialize DHT with temporary username
            self.dht = DHT(
                host=local_ip,
                port=DEFAULT_PORT,
                bootstrap_nodes=BOOTSTRAP_NODES,
                username="Anonymous",  # Will be updated after authentication
                db_manager=self.db_manager  # Pass database manager to DHT
            )
            self.logger.debug("DHT initialized")
            
            # Set file manager in DHT
            self.dht.set_file_manager(self.file_manager)
            self.logger.debug("File protocol initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing components: {e}")
            raise
    
    def get_local_ip(self) -> str:
        """Get the local IP address."""
        try:
            # Create a socket to determine the local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception as e:
            self.logger.warning(f"Could not determine local IP: {e}")
            return DEFAULT_HOST
    
    async def initialize(self):
        """Initialize async components."""
        self.logger.info("Initializing async components")
        try:
            # Initialize database
            await self.db_manager.initialize()
            self.logger.debug("Database initialized")
            
            # Start DHT network
            await self.dht.start()
            self.logger.debug("DHT network started")
            
            # Join network using bootstrap nodes
            if BOOTSTRAP_NODES:
                self.logger.info(f"Joining network using bootstrap nodes: {BOOTSTRAP_NODES}")
                await self.dht.join_network(BOOTSTRAP_NODES)
            else:
                self.logger.info("No bootstrap nodes configured, starting as first node")
        except Exception as e:
            self.logger.error(f"Error during async initialization: {e}")
            raise
    
    def process_events(self):
        """Process asyncio events."""
        try:
            self.loop.stop()
            self.loop.run_until_complete(self.process_async_events())
            self.loop.run_forever()
        except Exception as e:
            self.logger.error(f"Error processing events: {e}")
    
    async def process_async_events(self):
        """Process async events."""
        try:
            # Process DHT events
            if hasattr(self.dht, 'process_events'):
                await self.dht.process_events()
            # Process file transfer events
            if hasattr(self.file_manager, 'process_events'):
                await self.file_manager.process_events()
        except Exception as e:
            self.logger.error(f"Error processing async events: {e}")
    
    def on_auth_successful(self, user_id: str, username: str):
        """Handle successful authentication."""
        try:
            self.logger.info(f"User authenticated: {username} ({user_id})")
            
            # Update DHT username
            self.dht.username = username
            
            # Create main window
            self.main_window = MainWindow(
                file_manager=self.file_manager,
                network_manager=self.dht,
                db_manager=self.db_manager,
                user_id=user_id,
                username=username
            )
            self.main_window.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
            self.main_window.show()
            
            # Hide auth window
            self.auth_window.hide()
        except Exception as e:
            self.logger.error(f"Error creating main window: {e}")
            QMessageBox.critical(None, "Error", 
                               f"Failed to create main window: {str(e)}")
    
    def run(self):
        """Run the application."""
        try:
            self.logger.info("Starting application")
            
            # Initialize async components
            self.loop.run_until_complete(self.initialize())
            
            # Show auth window
            self.auth_window.show()
            
            # Run application
            return self.app.exec()
        except Exception as e:
            self.logger.error(f"Error running application: {e}")
            QMessageBox.critical(None, "Error", 
                               f"Failed to run application: {str(e)}")
            return 1
    
    def cleanup(self):
        """Clean up resources."""
        try:
            self.logger.info("Cleaning up resources")
            # Stop DHT network
            if hasattr(self.dht, 'stop'):
                self.loop.run_until_complete(self.dht.stop())
            self.logger.debug("DHT network stopped")
            
            # Close database connection
            self.loop.run_until_complete(self.db_manager.close())
            self.logger.debug("Database connection closed")
            
            # Stop event loop
            self.loop.stop()
            self.logger.debug("Event loop stopped")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

def main():
    """Main entry point."""
    app = P2PFileSharingApp()
    try:
        return app.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1
    finally:
        app.cleanup()

if __name__ == "__main__":
    sys.exit(main()) 