# University Data System

A comprehensive system for processing university and course data with semantic search capabilities.

## Features

- Extract structured data from raw university and course information
- Generate vector embeddings for semantic search
- Interactive chat interface for querying university and course data
- REST API for semantic search

## Prerequisites

- Python 3.8 or higher
- SQLite 3.38.0 or higher (recommended)
- Gemini API key

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/university_data_system.git
   cd university_data_system
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set your Gemini API key:
   ```bash
   export GEMINI_API_KEY="your_api_key_here"
   ```

## Usage

The system consists of several modules that can be run independently:

### 1. Process Raw Data

Process raw scraped data into structured data and store it in the database:

```bash
python main.py --mode process --raw-dir data/raw --db-path data/database.db
```

### 2. Generate Embeddings

Generate embeddings for university and course descriptions:

```bash
python main.py --mode embed --db-path data/database.db
```

### 3. Interactive Chat

Start an interactive chat session to query university and course information:

```bash
python main.py --mode chat --db-path data/database.db
```

### 4. Semantic Search API

Run the semantic search API:

```bash
python main.py --mode api --db-path data/database.db --host 0.0.0.0 --port 8000
```

## Vector Search Implementation

This system uses the `sqlite-vec` extension to implement vector similarity search in SQLite. The extension provides a virtual table mechanism for efficient vector search.

### How it works:

1. Text descriptions are converted to vector embeddings using Google's Gemini embedding model
2. Embeddings are stored as binary blobs in the SQLite database
3. Virtual tables (`vec_university` and `vec_course`) are created for vector search
4. When a user makes a query, the query is also converted to an embedding
5. The system finds the most similar university and course embeddings using the MATCH operator
6. Results are ranked by distance and returned to the user

## API Endpoints

The semantic search API provides the following endpoints:

- `GET /search?query=<query>&limit=<limit>&search_type=<type>`: Search for universities and courses
  - `query`: The search query
  - `limit` (optional): Maximum number of results to return (default: 5)
  - `search_type` (optional): Type of search - 'all', 'universities', or 'courses' (default: 'all')

## Project Structure

```
university_data_system/
├── data/
│   ├── raw/                  # Raw scraped data files
│   ├── processed/            # Processed data (if needed)
│   └── database.db           # SQLite database
├── modules/
│   ├── __init__.py
│   ├── structured_data_generator.py  # Process raw data
│   ├── embedding_generator.py        # Generate embeddings
│   ├── chat_module.py                # Interactive chat
│   └── semantic_search_api.py        # REST API
├── utils/
│   ├── __init__.py
│   ├── db_utils.py           # Database utilities
│   └── gemini_utils.py       # Gemini API utilities
├── main.py                   # Main entry point
└── requirements.txt          # Dependencies
```

