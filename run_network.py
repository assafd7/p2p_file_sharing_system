#!/usr/bin/env python3
import sys
import os
import socket
import argparse
from pathlib import Path

def get_local_ip():
    """Get the local IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        print(f"Error getting local IP: {e}")
        return "127.0.0.1"

def update_bootstrap_nodes(bootstrap_ip, bootstrap_port):
    """Update the bootstrap nodes in the configuration."""
    config_path = Path("src/config.py")
    if not config_path.exists():
        print("Error: config.py not found")
        return False
    
    with open(config_path, "r") as f:
        lines = f.readlines()
    
    # Find the BOOTSTRAP_NODES line
    for i, line in enumerate(lines):
        if "BOOTSTRAP_NODES" in line:
            # Update the bootstrap nodes
            if bootstrap_ip:
                lines[i] = f'BOOTSTRAP_NODES = [\n    ("{bootstrap_ip}", {bootstrap_port}),\n]\n'
            else:
                lines[i] = 'BOOTSTRAP_NODES = []\n'
            break
    
    with open(config_path, "w") as f:
        f.writelines(lines)
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Run P2P File Sharing System")
    parser.add_argument("--bootstrap", action="store_true", help="Run as bootstrap node")
    parser.add_argument("--connect", metavar="IP", help="Connect to bootstrap node IP")
    parser.add_argument("--port", type=int, default=8000, help="Port to use (default: 8000)")
    
    args = parser.parse_args()
    
    if args.bootstrap:
        # Run as bootstrap node
        local_ip = get_local_ip()
        print(f"Running as bootstrap node on {local_ip}:{args.port}")
        print("Share this IP address with other peers")
        
        # Clear bootstrap nodes
        if update_bootstrap_nodes("", 0):
            print("Configuration updated")
        else:
            print("Failed to update configuration")
            return 1
        
        # Run the application
        os.system(f"python run.py")
        
    elif args.connect:
        # Connect to bootstrap node
        print(f"Connecting to bootstrap node at {args.connect}:{args.port}")
        
        # Update bootstrap nodes
        if update_bootstrap_nodes(args.connect, args.port):
            print("Configuration updated")
        else:
            print("Failed to update configuration")
            return 1
        
        # Run the application
        os.system(f"python run.py")
        
    else:
        parser.print_help()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 