#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PaleoPal LLM Data Query Example

This script demonstrates how to use the LLM Data Query tool
with several example natural language queries and different LLM providers.
"""

import json
import logging
import os
from llm_data_query import LLMDataQuery

from dotenv import load_dotenv
from os.path import join, dirname

# Load Environment variables
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_example_query(llm_query, query, save_results=False):
    """
    Run an example query and print the results.
    
    Args:
        llm_query (LLMDataQuery): The LLM query handler
        query (str): The natural language query to run
        save_results (bool): Whether to save results to JSON
    """
    print("\n" + "="*80)
    print(f"QUERY: {query}")
    print("="*80)
    
    # Process the query
    dataset_names = llm_query.process_query(query)
    
    # Print results
    print(f"\nFound {len(dataset_names)} datasets matching the query criteria.")
    
    if dataset_names:
        print("\nDataset names:")
        for i, name in enumerate(dataset_names):
            print(f"  {i+1}. {name}")
            # Only show first 10 names if there are too many
            if i >= 9 and len(dataset_names) > 10:
                print(f"  ... and {len(dataset_names) - 10} more.")
                break
    
    # Save results if requested
    if save_results and dataset_names:
        # Create a safe filename from the query
        safe_name = query.replace(" ", "_").replace("/", "_").replace("?", "").lower()[:50]
        output_dir = "example_results"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save dataset names to JSON file
        output_file = os.path.join(output_dir, f"{safe_name}.json")
        with open(output_file, "w") as f:
            json.dump(dataset_names, f, indent=2)
        print(f"\nResults saved to {output_file}")
    
    return dataset_names

def main():
    """
    Run several example queries to demonstrate the LLM Data Query tool.
    """
    # Example queries to test
    example_queries = [
        "I want all coral records that represent temperature and d18O, annually resolved covering the Holocene period",
        "Find ice core records from Greenland that include temperature data",
        "Show me lake sediment records with pollen data from North America",
        "Get all speleothem records with annual resolution that cover the Common Era",
        "I need datasets that fully contain the entire Last Millennium period"
    ]
    
    # Try using different LLM providers
    providers_to_try = [
        {"provider": "ollama", "model": "deepseek-r1", "name": "Ollama with DeepSeek"},
        {"provider": "openai", "model": "gpt-3.5-turbo", "name": "OpenAI GPT-3.5"},
        {"provider": "claude", "model": "claude-3-haiku-20240307", "name": "Claude 3 Haiku"}
    ]
    
    for provider_info in providers_to_try:
        provider_type = provider_info["provider"]
        model_name = provider_info["model"]
        display_name = provider_info["name"]
        
        print("\n" + "="*80)
        print(f"TESTING WITH {display_name}")
        print("="*80)
        
        try:
            # Initialize the LLM Data Query tool with this provider
            llm_query = LLMDataQuery(
                provider_type=provider_type,
                model_name=model_name
            )
            
            # Run the first example query with this provider
            run_example_query(llm_query, example_queries[0], save_results=True)
            
            # If successful, try another query
            if len(example_queries) > 1:
                run_example_query(llm_query, example_queries[1], save_results=True)
                
            print(f"\n{display_name} testing completed successfully!")
            
            # If we found a working provider, we could just use it for all queries
            # Uncomment the following code to run all example queries with the first working provider
            """
            print("\nRunning all example queries with this provider...")
            for query in example_queries[2:]:
                run_example_query(llm_query, query, save_results=True)
            """
            
            # Break after finding a working provider
            break
            
        except Exception as e:
            print(f"\nError using {display_name}: {str(e)}")
            print("Trying next provider...\n")
    else:
        # This executes if the loop completed without a break (no working provider found)
        print("\nNo working LLM providers found. Please check your installation and API keys.")
        return
    
    print("\nExample queries completed!")

if __name__ == "__main__":
    main() 