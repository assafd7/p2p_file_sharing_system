#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from src.main import main

if __name__ == "__main__":
    # Add the project root to Python path
    project_root = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, project_root)
    
    # Run the application
    sys.exit(main()) 