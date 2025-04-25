#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLM Data Query Interface

A wrapper around data_query.py that allows an LLM (Language Learning Model)
to parse natural language queries and find paleoclimate datasets.
"""

import json
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple, Any

# Import DataQuery from data_query.py
from tools.data_query import DataQuery
from llm_providers import LLMProvider, LLMProviderFactory

from dotenv import load_dotenv
from os.path import join, dirname

# Load Environment variables
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("llm_data_query.log"),
    ]
)
logger = logging.getLogger(__name__)


class LLMDataQuery:
    """
    A class that uses an LLM to parse natural language queries for paleoclimate data
    and uses DataQuery to find matching datasets.
    """

    def __init__(
        self,
        provider_type: str = "ollama",
        model_name: Optional[str] = None,
        api_key: Optional[str] = None,
        endpoint_url: str = "https://linkedearth.graphdb.mint.isi.edu",
        repository: str = "LiPDVerse-dynamic"
    ):
        """
        Initialize the LLMDataQuery with an LLM provider and DataQuery.

        Args:
            provider_type (str): Type of LLM provider to use ('ollama', 'openai', 'claude')
            model_name (str, optional): The name of the model to use
            api_key (str, optional): API key for the provider (if applicable)
            endpoint_url (str): The URL of the GraphDB SPARQL endpoint
            repository (str): The name of the repository in GraphDB
        """
        # Initialize LLM provider
        self.provider_type = provider_type
        self.model_name = model_name
        self.api_key = api_key
        
        # Create the LLM provider
        self.llm_provider = self._initialize_provider()
        
        # Initialize DataQuery
        self.data_query = DataQuery(endpoint_url, repository)
        
        # Load the available parameters for reference by the LLM
        self.parameter_reference = self._load_parameter_reference()
        
        logger.info(f"Initialized LLMDataQuery with provider: {provider_type}")
        logger.info(f"GraphDB endpoint: {endpoint_url}, repository: {repository}")

    def _initialize_provider(self) -> LLMProvider:
        """
        Initialize the LLM provider. If the specified provider is not available,
        try to fall back to an available provider.
        
        Returns:
            LLMProvider: The initialized LLM provider
            
        Raises:
            RuntimeError: If no LLM providers are available
        """
        # Try to create the specified provider
        try:
            provider = LLMProviderFactory.create_provider(
                provider_type=self.provider_type,
                model_name=self.model_name,
                api_key=self.api_key
            )
            
            if provider.is_available():
                logger.info(f"Using LLM provider: {self.provider_type}")
                return provider
                
        except Exception as e:
            logger.warning(f"Error initializing {self.provider_type} provider: {str(e)}")
        
        # Try to find an available provider
        logger.info(f"Specified provider {self.provider_type} is not available. Trying to find an available provider...")
        provider = LLMProviderFactory.get_available_provider(
            preferred_providers=["ollama", "openai", "claude"],
            model_name=self.model_name,
            api_key=self.api_key
        )
        
        if provider:
            return provider
        
        # No providers available
        error_msg = "No LLM providers are available. Please install at least one provider package (ollama, openai, or anthropic)."
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    def _load_parameter_reference(self) -> Dict[str, Any]:
        """
        Load reference information about available parameters for the LLM.
        
        Returns:
            Dict[str, Any]: Dictionary of parameter information
        """
        # This could be loaded from a JSON file, but for simplicity, we'll define it here
        return {
            "archive_types": [
                "Coral", "GlacierIce", "LakeSediment", "MarineSediment", "MolluskShell", 
                "Sclerosponge", "Speleothem", "WoodOrTree"
            ],
            "common_variables": [
                "d18O", "d13C", "Sr/Ca", "Mg/Ca", "temperature", "precipitation", 
                "salinity", "pH", "radiocarbon"
            ],
            "common_interpretations": [
                "temperature", "precipitation", "seawater_temperature", "atmospheric_circulation",
                "sea_surface_temperature", "salinity", "ice_volume", "upwelling"
            ],
            "resolution_terms": [
                "annual", "decadal", "centennial", "millennial", "yr", "month", "seasonal"
            ],
            "time_periods": [
                "Holocene", "LateHolocene", "MidHolocene", "EarlyHolocene",
                "YoungerDryas", "LastGlacialMaximum", "LastMillennium", "CommonEra",
                "Industrial", "Pleistocene", "LastInterglacial"
            ],
            "regions": [
                "North America", "South America", "Europe", "Asia", "Africa", 
                "Australia", "Antarctica", "Pacific Ocean", "Atlantic Ocean", 
                "Indian Ocean", "Arctic Ocean", "Southern Ocean", "Tropics",
                "Mediterranean", "Caribbean"
            ],
            "time_overlap_types": [
                "partial", "containing", "contained"
            ]
        }

    def parse_query_with_llm(self, query: str) -> Dict[str, Any]:
        """
        Use the LLM to parse a natural language query into parameters for DataQuery.

        Args:
            query (str): The natural language query from the user

        Returns:
            Dict[str, Any]: Dictionary of extracted parameters
        """
        logger.info(f"Parsing query with LLM: {query}")
        
        # Prepare the prompt for the LLM
        prompt = self._prepare_llm_prompt(query)
        
        try:
            # Generate response using the provider
            llm_response = self.llm_provider.generate_response(prompt)
            logger.debug(f"LLM response: {llm_response}")
            
            # Extract the JSON part from the response
            params = self._extract_json_from_response(llm_response)
            logger.info(f"Extracted parameters: {params}")
            
            return params
            
        except Exception as e:
            logger.error(f"Error calling LLM: {str(e)}")
            return {}

    def _prepare_llm_prompt(self, query: str) -> str:
        """
        Prepare a prompt for the LLM to extract query parameters.

        Args:
            query (str): The natural language query from the user

        Returns:
            str: The formatted prompt for the LLM
        """
        # Convert parameter reference to JSON string
        param_ref_json = json.dumps(self.parameter_reference, indent=2)
        
        prompt = f"""
You are a specialized AI assistant that extracts structured parameters from natural language queries about paleoclimate data.

Here is the user's query: 
"{query}"

Extract the following parameters from the query (if mentioned):
1. archive_type: The type of paleoclimate archive (e.g., Coral, GlacierIce)
2. variables: List of variables mentioned (e.g., d18O, temperature)
3. interpretations: List of climate interpretations mentioned (e.g., temperature, precipitation)
4. resolution: Temporal resolution of the data (e.g., annual, decadal)
5. time_period: Time period of interest (e.g., Holocene, LastGlacialMaximum)
6. location: Geographic region of interest (e.g., Pacific Ocean, North America)
7. time_overlap: How the dataset should overlap with the time period (partial, containing, contained)

Here's reference information about available parameters:
{param_ref_json}

Respond ONLY with a JSON object containing the extracted parameters, like this:
{{
  "archive_type": "string or null",
  "variables": ["list", "of", "variables"] or null,
  "interpretations": ["list", "of", "interpretations"] or null,
  "resolution": "string or null",
  "time_period": "string or null",
  "location": "string or null", 
  "time_overlap": "string or null"
}}

Note that:
- Variables are direct measurements (like d18O, Sr/Ca)
- Interpretations are climate variables derived from measurements (like temperature, precipitation)
- Don't guess parameters that aren't mentioned in the query
- Format the parameters exactly as they appear in the reference
"""
        return prompt

    def _extract_json_from_response(self, response: str) -> Dict[str, Any]:
        """
        Extract the JSON object from the LLM response.

        Args:
            response (str): The LLM response text

        Returns:
            Dict[str, Any]: The extracted parameters as a dictionary
        """
        try:
            # Try to find JSON object in the response using regex
            json_match = re.search(r'({.*})', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                params = json.loads(json_str)
                return params
            else:
                # If no JSON object is found, try parsing the entire response
                params = json.loads(response)
                return params
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON from LLM response: {response}")
            return {}

    def process_query(self, query: str) -> List[str]:
        """
        Process a natural language query and return matching dataset names.

        Args:
            query (str): The natural language query from the user

        Returns:
            List[str]: List of dataset names matching the criteria
        """
        # Parse the query with the LLM
        params = self.parse_query_with_llm(query)
        
        if not params:
            logger.warning("Failed to extract parameters from the query.")
            return []
        
        # Process the query with DataQuery
        dataset_names = self.data_query.process_query(
            archive_type=params.get("archive_type"),
            variables=params.get("variables"),
            interpretations=params.get("interpretations"),
            resolution=params.get("resolution"),
            time_period=params.get("time_period"),
            location=params.get("location"),
            time_overlap=params.get("time_overlap", "partial")
        )
        
        return dataset_names


def main():
    """
    Main function for command-line execution.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Query paleoclimate data using natural language")
    
    # LLM parameters
    parser.add_argument("--provider", type=str, default="ollama",
                       help="LLM provider to use (ollama, openai, claude) (default: ollama)")
    parser.add_argument("--model", type=str, default=None,
                       help="Model name for the provider (default depends on provider)")
    parser.add_argument("--api-key", type=str, default=None,
                       help="API key for the provider (if needed)")
    
    # GraphDB connection parameters
    parser.add_argument("--endpoint", type=str, default="https://linkedearth.graphdb.mint.isi.edu",
                       help="GraphDB endpoint URL (default: https://linkedearth.graphdb.mint.isi.edu)")
    parser.add_argument("--repository", type=str, default="LiPDVerse-dynamic",
                       help="GraphDB repository name (default: LiPDVerse-dynamic)")
    
    # Query parameter
    parser.add_argument("--query", type=str, required=True,
                       help="Natural language query for paleoclimate data")
    
    # Output parameters
    parser.add_argument("--output-dir", type=str, default="data_query_results",
                       help="Directory to save results to (default: data_query_results)")
    parser.add_argument("--save-json", action="store_true",
                       help="Save the results as a JSON file")
    
    # Debugging parameter
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    # Create LLMDataQuery
    llm_query = LLMDataQuery(
        provider_type=args.provider,
        model_name=args.model,
        api_key=args.api_key,
        endpoint_url=args.endpoint,
        repository=args.repository
    )
    
    # Process the query
    dataset_names = llm_query.process_query(args.query)
    
    # Print results
    print(f"\nFound {len(dataset_names)} datasets matching all criteria.")
    
    if dataset_names:
        print("\nDataset names:")
        for i, name in enumerate(dataset_names):
            print(f"  - {name}")
            # Only show first 10 names if there are too many
            if i >= 9 and len(dataset_names) > 10:
                print(f"  ... and {len(dataset_names) - 10} more.")
                break
        
        # Save results if requested
        if args.save_json:
            # Create output directory if it doesn't exist
            if not os.path.exists(args.output_dir):
                os.makedirs(args.output_dir)
            
            # Save dataset names to JSON file
            output_file = os.path.join(args.output_dir, "dataset_names.json")
            with open(output_file, "w") as f:
                json.dump(dataset_names, f, indent=2)
            print(f"\nDataset names saved to {output_file}")


if __name__ == "__main__":
    main() 