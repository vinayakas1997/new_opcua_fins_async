#!/usr/bin/env python3
"""
Standalone OPC UA Node Manager
This service runs in a separate container and manages OPC UA node mapping.
It creates and maintains JSON files that other services can read from.
"""

import sys
import time
import logging
import logging.config
import json
import os
from opcua import Client
from opcua_json import OpcuaAutoNodeMapper
from datetime import datetime
import signal
import threading
from signal_manager import register_update_callback, start_signal_monitoring, stop_signal_monitoring

class OPCUANodeManager:
    def __init__(self, opcua_url, json_file_path='opcua_json_files/nodes.json', signal_based=True):
        """
        Initialize the OPC UA Node Manager
        
        Args:
            opcua_url: OPC UA server URL
            json_file_path: Path to store the JSON node mapping file
            signal_based: If True, updates only on signal; if False, uses periodic updates
        """
        self.opcua_url = opcua_url
        #Here we are defining the path for the opcua nodes to be saves 
        self.json_file_path = json_file_path
        self.signal_based = signal_based
        self.client = None
        self.mapper = None
        self.running = False
        self.update_requested = threading.Event()
        self.logger = logging.getLogger("OPCUANodeManager")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # Setup cross-platform update signal handling
        if signal_based:
            register_update_callback("node_manager", self._file_update_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        
    def _file_update_handler(self, signal_data):
        """Handle update signal from file-based system"""
        self.logger.info(f"Received update signal via file system at {signal_data.get('timestamp')}")
        self.update_requested.set()
        
    def connect(self):
        """Connect to OPC UA server and initialize node mapper"""
        try:
            self.logger.info(f"Connecting to OPC UA server: {self.opcua_url}")
            self.client = Client(self.opcua_url)
            self.client.connect()
            self.logger.info("Successfully connected to OPC UA server")
            
            # Create node mapper with initial scan
            self.logger.info("Creating initial node mapping...")
            self.mapper = OpcuaAutoNodeMapper(
                client=self.client,
                json_path=self.json_file_path,
                reload=True,  # Force initial scan
                console_print=True
            )
            
            # Create metadata file with connection info
            self._create_metadata_file()
            
            self.logger.info(f"Node mapping saved to: {self.json_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to OPC UA server: {e}")
            return False
    
    # def _create_metadata_file(self):
    #     """Create a metadata file with connection and update information"""
    #     try:
    #         with open(self.json_file_path, 'r') as f:
    #             nodes_data = json.load(f)
    #         node_count = len(nodes_data)
    #     except Exception:
    #         node_count = 0
    #     metadata = {
    #         "opcua_url": self.opcua_url,
    #         "json_file": self.json_file_path,
    #         "last_updated": datetime.now().isoformat(),
    #         "signal_based": self.signal_based,
    #         "node_count": node_count,
    #         "status": "connected"
    #     }
        
    #     metadata_file = self.json_file_path.replace('.json', '_metadata.json')
    #     with open(metadata_file, 'w') as f:
    #         json.dump(metadata, f, indent=4)
        
    #     self.logger.info(f"Metadata saved to: {metadata_file}")
    def _create_metadata_file(self):
        """Create a metadata file with connection and update information"""
        self.logger.info("=== _create_metadata_file() called ===")  # Entry point log
        
        try:
            self.logger.info(f"Reading nodes from: {self.json_file_path}")
            with open(self.json_file_path, 'r') as f:
                nodes_data = json.load(f)
            node_count = len(nodes_data)
            self.logger.info(f"Node count: {node_count}")
        except FileNotFoundError:
            self.logger.warning(f"Node file not found: {self.json_file_path}")
            node_count = 0
        except Exception as e:
            self.logger.error(f"Error reading node file: {e}", exc_info=True)
            node_count = 0
        
        metadata = {
            "opcua_url": self.opcua_url,
            "json_file": self.json_file_path,
            "last_updated": datetime.now().isoformat(),
            "signal_based": self.signal_based,
            "node_count": node_count,
            "status": "connected"
        }
        
        metadata_file = self.json_file_path.replace('.json', '_metadata.json')
        self.logger.info(f"Writing metadata to: {metadata_file}")
        
        try:
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=4)
            self.logger.info(f"✓ Metadata successfully saved to: {metadata_file}")
        except Exception as e:
            self.logger.error(f"✗ Failed to write metadata file: {e}", exc_info=True)
            raise  # Re-raise to propagate the error

    
    def update_node_mapping(self):
        """Update the node mapping by rescanning the OPC UA server"""
        try:
            if not self.client or not self.mapper:
                self.logger.error("Client or mapper not initialized")
                return False
                
            self.logger.info("Updating node mapping...")
            
            
            # Force reload to get latest nodes
            self.mapper._initialize_node_map(reload=True)
            
            # Update metadata
            self._create_metadata_file()
            
            self.logger.info(f"Node mapping updated: {len(self.mapper.node_map)} nodes")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update node mapping: {e}")
            return False
    
    def run(self):
        """Main run loop - signal-based or periodic update node mapping"""
        if not self.connect():
            return False
            
        self.running = True
        
        if self.signal_based:
            self.logger.info("Starting node manager in signal-based mode (waiting for file triggers)")
            # Start the cross-platform signal monitoring
            start_signal_monitoring()
            
            while self.running:
                try:
                    # Wait for update signal or check every second for shutdown
                    if self.update_requested.wait(timeout=1.0):
                        # Update signal received
                        self.update_node_mapping()
                        self.update_requested.clear()
                except Exception as e:
                    self.logger.error(f"Error in signal-based loop: {e}")
                    time.sleep(5)  # Brief pause before retrying
        else:
            # Fallback to periodic updates (legacy mode)
            update_interval = 300  # Default 5 minutes
            self.logger.info(f"Starting node manager in periodic mode with {update_interval}s interval")
            while self.running:
                try:
                    # Wait for update interval or shutdown signal
                    for _ in range(update_interval):
                        if not self.running:
                            break
                        time.sleep(1)
                    
                    if self.running:
                        # Periodic update check
                        self.update_node_mapping()
                        
                except Exception as e:
                    self.logger.error(f"Error in periodic loop: {e}")
                    time.sleep(5)  # Brief pause before retrying
                
        self.logger.info("Node manager stopped")
        return True
    def stop(self):
        """Stop the node manager"""
        self.running = False
        
        # Stop signal monitoring if it was started
        if self.signal_based:
            stop_signal_monitoring()
        
        if self.client:
            try:
                self.client.disconnect()
                self.logger.info("Disconnected from OPC UA server")
            except Exception as e:
                self.logger.error(f"Error disconnecting: {e}")
    
    def health_check(self):
        """Perform a health check"""
        try:
            if not self.client:
                return False
                
            # Try to get server state
            server_node = self.client.get_server_node()
            server_node.get_browse_name()
            return True
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False

def main():
    """Main entry point"""
    # Configure logging
    try:
        logging.config.fileConfig("logging.conf")
    except:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    # We have the two loggers name as the OPCUANodeManager and main, so I converted to teh OPCUANodeManager-main to understand both are from 
    # the same file and one is the main function and the other is the class
    logger = logging.getLogger("OPCUANodeManager-main")
    
    # Get configuration from environment variables or command line
    # opcua_url = os.getenv('OPCUA_SERVER_URL', 'opc.tcp://localhost:4840')
    opcua_url=os.getenv('OPCUA_SERVER_URL', 'opc.tcp://192.168.1.20:4840')
    json_file = os.getenv('JSON_FILE_NAME', 'opcua_json_files/nodes.json')
    signal_based = os.getenv('SIGNAL_BASED', 'true').lower() == 'true'  # Default to signal-based
    
    # Override with command line arguments if provided
    if len(sys.argv) > 1:
        opcua_url = sys.argv[1]
    if len(sys.argv) > 2:
        json_file = sys.argv[2]
    if len(sys.argv) > 3:
        signal_based = sys.argv[3].lower() == 'true'
    
    # # Ensure JSON file is in the shared directory
    # if not json_file.startswith('/app/opcua_json_files/'):
    #     json_file = f'/app/opcua_json_files/{json_file}'
    
    logger.info(f"Starting OPC UA Node Manager")
    logger.info(f"OPC UA Server: {opcua_url}")
    logger.info(f"JSON File: {json_file}")
    logger.info(f"Signal Based: {signal_based}")
    
    # Create and run the node manager
    manager = OPCUANodeManager(opcua_url, json_file, signal_based)
    
    try:
        success = manager.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        manager.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        manager.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
