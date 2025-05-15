#!/usr/bin/env python3
"""
Setup script for installing and testing the SQLite VSS extension.
"""

import os
import sys
import platform
import subprocess
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def check_sqlite_version():
    """Check if SQLite version is compatible with VSS extension."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT sqlite_version()")
    version = cursor.fetchone()[0]
    conn.close()
    
    logger.info(f"SQLite version: {version}")
    
    # Parse version
    major, minor, patch = map(int, version.split('.'))
    
    if major < 3 or (major == 3 and minor < 38):
        logger.warning("SQLite version 3.38.0 or higher is recommended for VSS extension")
        return False
    
    return True

def install_vss_extension():
    """Install the SQLite VSS extension."""
    system = platform.system().lower()
    
    if system == "linux":
        try:
            # Try to install using pip first
            logger.info("Attempting to install sqlite-vss using pip...")
            subprocess.run([sys.executable, "-m", "pip", "install", "sqlite-vss"], check=True)
            logger.info("sqlite-vss installed successfully via pip")
            return True
        except subprocess.CalledProcessError:
            logger.warning("Failed to install via pip, trying manual installation...")
            
            # Manual installation for Linux
            try:
                # Clone the repository
                subprocess.run(["git", "clone", "https://github.com/asg017/sqlite-vss.git"], check=True)
                
                # Build the extension
                os.chdir("sqlite-vss")
                subprocess.run(["make"], check=True)
                
                # Copy the extension to a system path
                subprocess.run(["sudo", "cp", "vss0.so", "/usr/local/lib/"], check=True)
                
                logger.info("SQLite VSS extension installed manually")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install VSS extension manually: {str(e)}")
                return False
    
    elif system == "darwin":  # macOS
        try:
            # Try to install using pip first
            logger.info("Attempting to install sqlite-vss using pip...")
            subprocess.run([sys.executable, "-m", "pip", "install", "sqlite-vss"], check=True)
            logger.info("sqlite-vss installed successfully via pip")
            return True
        except subprocess.CalledProcessError:
            logger.warning("Failed to install via pip, trying homebrew...")
            
            # Try using Homebrew
            try:
                subprocess.run(["brew", "install", "asg017/sqlite-vss/sqlite-vss"], check=True)
                logger.info("SQLite VSS extension installed via Homebrew")
                return True
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install VSS extension via Homebrew: {str(e)}")
                return False
    
    elif system == "windows":
        # For Windows, pip is the best option
        try:
            logger.info("Attempting to install sqlite-vss using pip...")
            subprocess.run([sys.executable, "-m", "pip", "install", "sqlite-vss"], check=True)
            logger.info("sqlite-vss installed successfully via pip")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install VSS extension via pip: {str(e)}")
            return False
    
    else:
        logger.error(f"Unsupported operating system: {system}")
        return False

def test_vss_extension():
    """Test if the VSS extension is working properly."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.enable_load_extension(True)
        
        # Try different possible paths
        extension_paths = [
            'sqlite-vss',
            './sqlite-vss',
            './vss0',
            '/usr/local/lib/sqlite-vss',
            '/usr/lib/sqlite-vss',
            'C:\\sqlite-vss',
        ]
        
        for path in extension_paths:
            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT load_extension('{path}')")
                
                # Test if we can create a VSS virtual table
                cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS vss_test USING vss0(
                    embedding(3),
                    id UNINDEXED
                )
                """)
                
                # Insert a test vector
                cursor.execute("INSERT INTO vss_test(embedding, id) VALUES (?, ?)", 
                              ([0.1, 0.2, 0.3], 1))
                
                # Test similarity search
                cursor.execute("SELECT vss_similarity(embedding, ?) FROM vss_test", 
                              ([0.1, 0.2, 0.3],))
                
                similarity = cursor.fetchone()[0]
                logger.info(f"VSS test successful! Similarity: {similarity}")
                return True
                
            except sqlite3.OperationalError as e:
                logger.debug(f"Failed to load VSS extension from {path}: {str(e)}")
        
        logger.error("Failed to load and test VSS extension from any path")
        return False
        
    except Exception as e:
        logger.error(f"Error testing VSS extension: {str(e)}")
        return False
    finally:
        conn.close()

def main():
    """Main function to set up the VSS extension."""
    logger.info("Starting SQLite VSS extension setup")
    
    # Check SQLite version
    if not check_sqlite_version():
        logger.warning("Continuing with installation despite SQLite version warning")
    
    # Install VSS extension
    if install_vss_extension():
        logger.info("VSS extension installation completed")
    else:
        logger.error("Failed to install VSS extension")
        return 1
    
    # Test VSS extension
    if test_vss_extension():
        logger.info("VSS extension test passed")
    else:
        logger.error("VSS extension test failed")
        return 1
    
    logger.info("SQLite VSS extension setup completed successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 