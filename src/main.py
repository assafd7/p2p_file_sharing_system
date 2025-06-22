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
    
    async def initialize_async_components(self):
        """Initialize async components (database, network, etc.)."""
        try:
            self.create_data_directories()

            self.security_manager = SecurityManager()
            self.logger.debug("Security manager initialized")
            
            self.db_manager = DatabaseManager(DB_PATH)
            await self.db_manager.initialize()
            self.logger.debug("Database manager initialized")
            
            local_ip = self.get_local_ip()
            self.logger.info(f"Local IP address: {local_ip}")
            
            self.dht = DHT(
                host=local_ip,
                port=DEFAULT_PORT,
                bootstrap_nodes=BOOTSTRAP_NODES,
                username="Anonymous",
                db_manager=self.db_manager
            )
            await self.dht.start()
            self.logger.debug("DHT initialized and started")
            
            self.file_manager = FileManager(
                storage_dir=FILES_DIR,
                temp_dir=TEMP_DIR,
                cache_dir=CACHE_DIR,
                db_manager=self.db_manager,
                dht=self.dht
            )
            self.logger.debug("File manager initialized")
            
            if BOOTSTRAP_NODES:
                await self.dht.join_network(BOOTSTRAP_NODES)
                self.logger.info(f"Joined network using bootstrap nodes: {BOOTSTRAP_NODES}")
            else:
                self.logger.info("No bootstrap nodes configured, starting as first node")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error during async initialization: {e}", exc_info=True)
            QMessageBox.critical(None, "Initialization Error", f"Failed to initialize application: {str(e)}")
            return False

    def create_gui_components(self):
        """Create GUI components."""
        try:
            self.auth_window = AuthWindow(self.db_manager, self.security_manager)
            self.auth_window.auth_successful.connect(self.on_auth_successful)
            self.auth_window.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
            self.logger.debug("Auth window created")
            return True
        except Exception as e:
            self.logger.error(f"Error creating GUI components: {e}", exc_info=True)
            return False

    def on_auth_successful(self, user_id: str, username: str):
        """Handle successful authentication."""
        try:
            self.logger.info(f"User authenticated: {username} ({user_id})")
            
            self.dht.username = username
            
            self.main_window = MainWindow(
                file_manager=self.file_manager,
                network_manager=self.dht,
                db_manager=self.db_manager,
                user_id=user_id,
                username=username
            )
            self.main_window.setMinimumSize(WINDOW_MIN_WIDTH, WINDOW_MIN_HEIGHT)
            self.main_window.show()
            
            self.auth_window.hide()
            self.logger.debug("Main window created and shown")
            
        except Exception as e:
            self.logger.error(f"Error creating main window: {e}", exc_info=True)
            QMessageBox.critical(None, "Error", f"Failed to create main window: {str(e)}")

    async def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up resources")
        try:
            if hasattr(self, 'dht') and self.dht:
                await self.dht.stop()
                self.logger.debug("DHT stopped")
            if hasattr(self, 'db_manager') and self.db_manager:
                await self.db_manager.close()
                self.logger.debug("Database closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        self.logger.info("Cleanup complete")

def main():
    """Main entry point for the application."""
    app_instance = P2PFileSharingApp()
    loop = qasync.QEventLoop(app_instance.app)
    asyncio.set_event_loop(loop)

    async def main_async():
        """Main async function."""
        try:
            # Initialize async components
            if not await app_instance.initialize_async_components():
                return 1
            
            # Create GUI components
            if not app_instance.create_gui_components():
                return 1
            
            # Show auth window
            app_instance.auth_window.show()
            
            # Create a future that will be cancelled when the app closes
            # This keeps the event loop running properly
            app_future = asyncio.Future()
            
            # Set up a callback to cancel the future when the app is about to quit
            def on_about_to_quit():
                if not app_future.done():
                    app_future.cancel()
            
            app_instance.app.aboutToQuit.connect(on_about_to_quit)
            
            # Wait for the future to be cancelled (when app closes)
            try:
                await app_future
            except asyncio.CancelledError:
                # This is expected when the app closes
                pass
            
            return 0
            
        except Exception as e:
            app_instance.logger.error(f"Application error: {e}", exc_info=True)
            return 1
        finally:
            await app_instance.cleanup()
    
    try:
        return loop.run_until_complete(main_async())
    except KeyboardInterrupt:
        app_instance.logger.info("Application interrupted by user")
        return 0
    except Exception as e:
        app_instance.logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        loop.close()
        app_instance.logger.info("Application exited")

if __name__ == "__main__":
    sys.exit(main()) 