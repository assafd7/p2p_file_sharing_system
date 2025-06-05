import sys
import os
import asyncio
import socket
from pathlib import Path
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QTimer

from src.ui.main_window import MainWindow
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
        
        # Create data directories
        self.create_data_directories()
        
        # Initialize components
        self.initialize_components()
        
        # Create Qt application
        self.app = QApplication(sys.argv)
        self.app.setApplicationName(WINDOW_TITLE)
        
        # Create main window
        self.main_window = MainWindow(
            file_manager=self.file_manager,
            network_manager=self.dht,
            db_manager=self.db_manager
        )
        self.main_window.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
        
        # Set up asyncio event loop
        self.loop = asyncio.get_event_loop()
        self.timer = QTimer()
        self.timer.timeout.connect(self.process_events)
        self.timer.start(100)  # Process events every 100ms
    
    def create_data_directories(self):
        """Create necessary data directories."""
        self.logger.debug("Creating data directories")
        
        for directory in [TEMP_DIR, FILES_DIR, CACHE_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Created directory: {directory}")
    
    def initialize_components(self):
        """Initialize application components."""
        self.logger.info("Initializing application components")
        
        # Initialize security manager
        self.security_manager = SecurityManager()
        self.logger.debug("Security manager initialized")
        
        # Initialize file manager
        self.file_manager = FileManager(FILES_DIR)
        self.logger.debug("File manager initialized")
        
        # Initialize database manager
        self.db_manager = DatabaseManager(DB_PATH)
        self.logger.debug("Database manager initialized")
        
        # Get local IP address
        local_ip = self.get_local_ip()
        self.logger.info(f"Local IP address: {local_ip}")
        
        # Initialize DHT
        self.dht = DHT(
            node_id=self.security_manager.generate_node_id(),
            host=local_ip,
            port=DEFAULT_PORT
        )
        self.logger.debug("DHT initialized")
    
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
        
        # Initialize database
        await self.db_manager.initialize()
        self.logger.debug("Database initialized")
        
        # Start DHT network
        await self.dht.start()
        self.logger.info("DHT network started")
        
        # Join network using bootstrap nodes
        if BOOTSTRAP_NODES:
            self.logger.info(f"Joining network using bootstrap nodes: {BOOTSTRAP_NODES}")
            await self.dht.join_network(BOOTSTRAP_NODES)
        else:
            self.logger.info("No bootstrap nodes configured, starting as first node")
    
    def process_events(self):
        """Process asyncio events."""
        self.loop.stop()
        self.loop.run_until_complete(self.process_async_events())
        self.loop.run_forever()
    
    async def process_async_events(self):
        """Process async events."""
        # Process DHT events
        await self.dht.process_events()
        
        # Process file transfer events
        await self.file_manager.process_events()
    
    def run(self):
        """Run the application."""
        self.logger.info("Starting application")
        
        # Initialize async components
        self.loop.run_until_complete(self.initialize())
        
        # Show main window
        self.main_window.show()
        
        # Run application
        return self.app.exec()
    
    def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up resources")
        
        # Stop DHT network
        self.loop.run_until_complete(self.dht.stop())
        self.logger.debug("DHT network stopped")
        
        # Close database connection
        self.loop.run_until_complete(self.db_manager.close())
        self.logger.debug("Database connection closed")
        
        # Stop event loop
        self.loop.stop()
        self.logger.debug("Event loop stopped")

def main():
    """Main entry point."""
    app = P2PFileSharingApp()
    try:
        return app.run()
    finally:
        app.cleanup()

if __name__ == "__main__":
    sys.exit(main()) 