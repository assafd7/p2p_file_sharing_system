import sys
import os
import asyncio
import socket
from pathlib import Path
from PyQt6.QtWidgets import QApplication, QMessageBox
from PyQt6.QtCore import QTimer
import logging
import qasync

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

# Add this before DatabaseManager is instantiated
print(f"[DEBUG] Resolved DB_PATH: {DB_PATH} (absolute: {DB_PATH.resolve()})")
logging.basicConfig(level=logging.DEBUG)

class P2PFileSharingApp:
    def __init__(self):
        """Initialize the P2P file sharing application."""
        self.logger = get_logger("main")
        self.logger.info("Initializing P2P file sharing application")
        
        setup_logging()
        self.app = QApplication(sys.argv)
        self.app.setApplicationName(WINDOW_TITLE)

        self.db_manager = None
        self.security_manager = None
        self.dht = None
        self.file_manager = None
        self.auth_window = None
        self.main_window = None
    
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
        """Create and initialize all application components asynchronously."""
        try:
            self.create_data_directories()

            self.security_manager = SecurityManager()
            self.db_manager = DatabaseManager(DB_PATH)
            await self.db_manager.initialize()

            local_ip = self.get_local_ip()
            self.dht = DHT(
                host=local_ip,
                port=DEFAULT_PORT,
                bootstrap_nodes=BOOTSTRAP_NODES,
                username="Anonymous",
                db_manager=self.db_manager
            )
            await self.dht.start()
            if BOOTSTRAP_NODES:
                await self.dht.join_network(BOOTSTRAP_NODES)

            self.file_manager = FileManager(
                storage_dir=FILES_DIR,
                temp_dir=TEMP_DIR,
                cache_dir=CACHE_DIR,
                db_manager=self.db_manager,
                dht=self.dht
            )

            self.auth_window = AuthWindow(self.db_manager, self.security_manager)
            self.auth_window.auth_successful.connect(self.on_auth_successful)
            return True
        except Exception as e:
            self.logger.error(f"Error during initialization: {e}", exc_info=True)
            QMessageBox.critical(None, "Initialization Error", f"Failed to initialize application: {str(e)}")
            return False
    
    def on_auth_successful(self, user_id: str, username: str):
        """Handle successful authentication."""
        self.logger.info(f"User authenticated: {username} ({user_id})")
        self.dht.username = username

        self.main_window = MainWindow(
            file_manager=self.file_manager,
            network_manager=self.dht,
            db_manager=self.db_manager,
            user_id=user_id,
            username=username
        )
        self.main_window.show()
        self.auth_window.hide()

    async def run(self):
        """Run the application."""
        if not await self.initialize():
            return 1

        self.auth_window.show()
        return await self.app.exec()

    async def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up resources")
        if self.dht:
            await self.dht.stop()
        if self.db_manager:
            await self.db_manager.close()
        self.logger.info("Cleanup complete")

def main():
    """Main entry point for the application."""
    app_instance = P2PFileSharingApp()
    loop = qasync.QEventLoop(app_instance.app)
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(app_instance.run())
    except KeyboardInterrupt:
        app_instance.logger.info("Application interrupted by user")
    finally:
        loop.run_until_complete(app_instance.cleanup())
        loop.close()
        app_instance.logger.info("Application exited")

if __name__ == "__main__":
    sys.exit(main()) 