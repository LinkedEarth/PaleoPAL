# PaleoPal LLM Data Query Tool

A tool that allows language models to query paleoclimate datasets using natural language.

## Overview

This tool enables LLMs to parse natural language queries about paleoclimate data and find matching datasets from the LinkedEarth GraphDB repository. It serves as a bridge between user queries and structured data access.

## Features

- Natural language understanding of paleoclimate data queries
- Extraction of parameters like archive type, variables, time periods, etc.
- Integration with the LinkedEarth GraphDB repository
- Support for multiple LLM providers:
  - Ollama (local models like deepseek, llama2)
  - OpenAI (GPT models)
  - Anthropic (Claude models)
- Automatic fallback to available providers
- Command-line interface for easy usage

## Requirements

- Python 3.7+
- One or more of the following LLM providers:
  - ollama (for local models)
  - openai (for GPT models)
  - anthropic (for Claude models)
- SPARQLWrapper (for GraphDB access)

## Installation

1. Clone this repository:

```bash
git clone https://github.com/yourusername/paleopal.git
cd paleopal
```

2. Install the required dependencies:

```bash
# Core dependencies
pip install SPARQLWrapper

# Install at least one of these LLM providers
pip install ollama  # For local models
pip install openai  # For OpenAI models
pip install anthropic  # For Claude models
```

3. Set up your LLM provider:

- **Ollama**: Install Ollama and pull the required model
  ```bash
  # Install Ollama (see https://ollama.com/download)
  # Pull a model
  ollama pull deepseek-r1
  ```

- **OpenAI**: Set your API key
  ```bash
  export OPENAI_API_KEY=your_api_key_here
  ```

- **Claude**: Set your API key
  ```bash
  export ANTHROPIC_API_KEY=your_api_key_here
  ```

## Usage

### Command-line Interface

You can use the tool directly from the command line:

```bash
# Using Ollama (default)
python llm_data_query.py --query "I want all coral records that represent temperature and d18O, annually resolved covering the Holocene period"

# Using OpenAI
python llm_data_query.py --provider openai --model gpt-3.5-turbo --query "Find ice core records from Greenland"

# Using Claude
python llm_data_query.py --provider claude --model claude-3-haiku-20240307 --query "Show me lake sediment records with pollen data"
```

Additional options:

```
--provider PROVIDER    LLM provider to use (ollama, openai, claude) (default: ollama)
--model MODEL          Model name for the provider (default depends on provider)
--api-key API_KEY      API key for the provider (if needed)
--endpoint ENDPOINT    GraphDB endpoint URL
--repository REPOSITORY GraphDB repository name
--output-dir OUTPUT_DIR Directory to save results to
--save-json             Save the results as a JSON file
--debug                 Enable debug logging
```

### Python API

You can also use the tool programmatically in your Python code:

```python
from llm_data_query import LLMDataQuery

# Initialize with Ollama (default)
llm_query = LLMDataQuery()

# Or initialize with OpenAI
llm_query = LLMDataQuery(
    provider_type="openai",
    model_name="gpt-3.5-turbo",
    api_key="your_api_key_here"
)

# Or initialize with Claude
llm_query = LLMDataQuery(
    provider_type="claude",
    model_name="claude-3-haiku-20240307",
    api_key="your_api_key_here"
)

# Process a natural language query
dataset_names = llm_query.process_query(
    "I want all coral records that represent temperature and d18O, annually resolved covering the Holocene period"
)

# Print the results
print(f"Found {len(dataset_names)} datasets")
for name in dataset_names:
    print(f"- {name}")
```

### Provider Fallback

The tool automatically tries to find an available LLM provider if the specified one is not available:

```python
# This will try to use OpenAI first, but if not available (no API key, etc.),
# it will try Ollama, and then Claude
llm_query = LLMDataQuery(provider_type="openai")
```

## Example Queries

The tool can handle various types of natural language queries:

- "Show me all coral records from the Pacific Ocean"
- "Find all annually resolved temperature records from the last millennium"
- "I need lake sediment cores containing d18O data from North America"
- "Get all Holocene speleothem records with annual resolution"

## How It Works

1. The tool takes a natural language query as input
2. It uses an LLM to extract structured parameters from the query
3. These parameters are passed to the DataQuery class to build and execute a SPARQL query
4. The matching dataset names are returned to the user

## Parameters That Can Be Extracted

- **archive_type**: Type of paleoclimate archive (e.g., Coral, GlacierIce)
- **variables**: Direct measurements (e.g., d18O, Sr/Ca)
- **interpretations**: Climate variables derived from measurements (e.g., temperature, precipitation)
- **resolution**: Temporal resolution of the data (e.g., annual, decadal)
- **time_period**: Time period of interest (e.g., Holocene, LastGlacialMaximum)
- **location**: Geographic region of interest (e.g., Pacific Ocean, North America)
- **time_overlap**: How the dataset should overlap with the time period (partial, containing, contained)

## Adding a New LLM Provider

To add support for a new LLM provider:

1. Implement a new provider class in `llm_providers.py` that inherits from `LLMProvider`
2. Implement the required methods: `is_available()` and `generate_response()`
3. Add the new provider to the `LLMProviderFactory` class

Example of a new provider implementation:

```python
class MyNewProvider(LLMProvider):
    def __init__(self, model_name, api_key=None):
        self.model_name = model_name
        self.api_key = api_key or os.environ.get("MY_PROVIDER_API_KEY")
    
    def is_available(self) -> bool:
        # Check if the provider is available
        return self.api_key is not None
    
    def generate_response(self, prompt: str) -> str:
        # Implement the logic to call your LLM provider
        # Return the text response
        return "Provider response text"
```

## License

[Your license information] 