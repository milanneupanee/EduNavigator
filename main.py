import os
import argparse
import logging
from pathlib import Path
import dotenv

from modules.structured_data_generator import StructuredDataGenerator
from modules.chat_module import ChatModule
from modules.semantic_search_api import run_api
from utils.db_utils import initialize_database


dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories if they don't exist."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)

def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description="University and Course Data Processing System")
    parser.add_argument("--mode", type=str, required=True, choices=["process", "chat", "api"],
                        help="Mode to run: process (structured data), chat (interactive chat), api (run API)")
    parser.add_argument("--raw-dir", type=str, default="data/raw", help="Directory containing raw scraped files")

    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host for API server")
    parser.add_argument("--port", type=int, default=8000, help="Port for API server")
    
    args = parser.parse_args()
    
    # Create necessary directories
    setup_directories()
    
    # Check if GEMINI_API_KEY is set
    if not os.environ.get("GEMINI_API_KEY"):
        logger.error("GEMINI_API_KEY environment variable not set. Please set it before running the application.")
        return

    # Ensure database is initialized
    initialize_database()
    
    # Run the appropriate module based on the mode
    if args.mode == "process":
        logger.info("Running Structured Data Generator")
        generator = StructuredDataGenerator(args.raw_dir)
        generator.process_files()
        
    elif args.mode == "chat":
        logger.info("Running Chat Module")
        chat_module = ChatModule()
        
        print("University and Course Chat Assistant")
        print("Type 'exit' or 'quit' to end the conversation")
        
        while True:
            query = input("\nYour query: ")
            
            if query.lower() in ['exit', 'quit']:
                break
                
            response = chat_module.process_query(query)
            print(f"\nAssistant: {response}")
            
    elif args.mode == "api":
        logger.info(f"Running Semantic Search API on {args.host}:{args.port}")
        run_api(args.host, args.port)

if __name__ == "__main__":
    main() 

