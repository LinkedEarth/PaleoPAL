#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Query Tool

A comprehensive toolkit for querying paleoclimate data from GraphDB and LiPD.
This script provides a flexible framework for constructing SPARQL queries
and filtering LiPD datasets based on various criteria.
"""

import argparse
import json
import logging
import os
import sys
from typing import List, Optional


from SPARQLWrapper import JSON, SPARQLWrapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data_query.log"),
    ]
)
logger = logging.getLogger(__name__)


class DataQuery:
    """
    A class to query paleoclimate data from GraphDB and LiPD.
    """

    def __init__(self, endpoint_url: str, repository: str):
        """
        Initialize the DataQuery with GraphDB endpoint and repository.

        Args:
            endpoint_url (str): The URL of the GraphDB SPARQL endpoint.
            repository (str): The name of the repository in GraphDB.
        """
        self.endpoint_url = endpoint_url
        self.repository = repository
        self.sparql_endpoint = f"{endpoint_url}/repositories/{repository}"
        self.sparql = SPARQLWrapper(self.sparql_endpoint)
        self.sparql.setReturnFormat(JSON)

        # Dictionary of ontology prefixes for SPARQL queries
        self.prefixes = {
            "rdf": "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
            "rdfs": "<http://www.w3.org/2000/01/rdf-schema#>",
            "le": "<http://linked.earth/ontology#>",
            "archive": "<http://linked.earth/ontology/archive#>",
            "proxy": "<http://linked.earth/ontology/paleo_proxy#>",
            "pvar": "<http://linked.earth/ontology/paleo_variables#>",
            "punits": "<http://linked.earth/ontology/paleo_units#>",
            "interp": "<http://linked.earth/ontology/interpretation#>",
            "cproxy": "<http://linked.earth/ontology/chron_proxy#>",
            "cunits": "<http://linked.earth/ontology/chron_units#>",
            "cvar": "<http://linked.earth/ontology/chron_variables#>",
        }

    def test_connection(self) -> bool:
        """
        Test the connection to the GraphDB endpoint.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            test_query = """
            SELECT (COUNT(*) as ?count) 
            WHERE { 
                ?s ?p ?o 
            } 
            LIMIT 1
            """
            self.sparql.setQuery(test_query)
            results = self.sparql.query().convert()
            count = int(results["results"]["bindings"][0]["count"]["value"])
            logger.info(f"Successfully connected to GraphDB. Repository contains data (count: {count}).")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GraphDB: {str(e)}")
            return False

    def build_prefixes(self) -> str:
        """
        Build the PREFIX section of a SPARQL query.

        Returns:
            str: PREFIX declarations for the SPARQL query
        """
        return "\n".join([f"PREFIX {prefix}: {uri}" for prefix, uri in self.prefixes.items()])

    def build_archive_filter(self, archive_type: str) -> str:
        """
        Build a SPARQL filter for archive type.

        Args:
            archive_type (str): The type of archive (e.g., "Coral", "GlacierIce")

        Returns:
            str: SPARQL query fragment for the archive type filter
        """
        return f"""
          ?dataset rdf:type le:Dataset ;
                  le:hasName ?datasetName ;
                  le:hasArchiveType archive:{archive_type} .
        """

    def build_variable_filter(self, variable_type: str, variable_name: str) -> str:
        """
        Build a SPARQL filter for a variable.

        Args:
            variable_type (str): The type of variable (e.g., "pvar" for paleo variables)
            variable_name (str): The name of the variable (e.g., "d18O", "temperature")

        Returns:
            str: SPARQL query fragment for the variable filter
        """
        # Generate a unique suffix for the variables in the query
        suffix = variable_name.replace("/", "_").lower()
        
        return f"""
          ?dataset le:hasPaleoData ?paleoData_{suffix} .
          ?paleoData_{suffix} le:hasMeasurementTable ?dataTable_{suffix} .
          ?dataTable_{suffix} le:hasVariable ?variable_{suffix} .
          ?variable_{suffix} le:hasStandardVariable {variable_type}:{variable_name} .
        """

    def build_interpretation_filter(self, interp_variable: str) -> str:
        """
        Build a SPARQL filter for an interpretation variable.

        Args:
            interp_variable (str): The name of the interpretation variable (e.g., "temperature")

        Returns:
            str: SPARQL query fragment for the interpretation filter
        """
        # Generate a unique suffix for the variables in the query
        suffix = interp_variable.replace("/", "_").lower()
        
        return f"""
          ?dataset le:hasPaleoData ?paleoData_interp_{suffix} .
          ?paleoData_interp_{suffix} le:hasMeasurementTable ?dataTable_interp_{suffix} .
          ?dataTable_interp_{suffix} le:hasVariable ?variable_interp_{suffix} .
          ?variable_interp_{suffix} le:hasInterpretation ?interpretation_{suffix} .
          ?interpretation_{suffix} le:hasVariable interp:{interp_variable} .
        """

    def build_resolution_filter(self, resolution_type: str) -> str:
        """
        Build a SPARQL filter for resolution.

        Args:
            resolution_type (str): The type of resolution (e.g., "annual", "decadal")

        Returns:
            str: SPARQL query fragment for the resolution filter
        """
        # Parse resolution terms
        resolution_terms = resolution_type.lower().split("|")
        
        # Different resolution handling depending on the terms
        if any(term in ["annual", "yr", "year"] for term in resolution_terms):
            # For annual resolution
            return self.build_annual_resolution_filter()
        elif any(term in ["decadal", "decade"] for term in resolution_terms):
            # For decadal resolution
            return self.build_decadal_resolution_filter()
        else:
            # For other resolutions, use a more general text-based approach
            filter_conditions = " || ".join([f"CONTAINS(LCASE(?resolutionLabel), '{term}')" for term in resolution_terms])
            return f"""
              ?variable_res le:hasResolution ?resolution .
              ?resolution rdfs:label ?resolutionLabel .
              FILTER({filter_conditions})
            """

    def build_annual_resolution_filter(self) -> str:
        """
        Build a specialized filter for annual resolution.
        
        Returns:
            str: SPARQL query fragment for annual resolution filter
        """
        return """
          ?variable_res le:hasResolution ?resolution .
          {
            # Option 1: Resolution has year units and max value is at most 1 year
            # (includes more fine-grained resolutions like monthly or daily)
            ?resolution le:hasUnits ?units ;
                        le:hasMaxValue ?maxValue .
            # Check for different types of year units
            FILTER(?units IN (punits:yr_AD, punits:yr_BP, punits:yr))
            FILTER(?maxValue <= 1)
          }
          UNION
          {
            # Option 2: Resolution is specified in months or days
            ?resolution le:hasUnits ?units .
            FILTER(?units IN (punits:month, punits:day, punits:hour))
          }
        """

    def build_decadal_resolution_filter(self) -> str:
        """
        Build a specialized filter for decadal resolution.
        
        Returns:
            str: SPARQL query fragment for decadal resolution filter
        """
        return """
          ?variable_res le:hasResolution ?resolution .
          {
            # Option 1: Resolution has year units and max value is at most 10 years
            # (includes more fine-grained resolutions like annual)
            ?resolution le:hasUnits ?units ;
                        le:hasMaxValue ?maxValue .
            # Check for different types of year units
            FILTER(?units IN (punits:yr_AD, punits:yr_BP, punits:yr))
            FILTER(?maxValue <= 10)
          }
          UNION
          {
            # Option 2: Resolution is specified in months, days, or hours (finer than decadal)
            ?resolution le:hasUnits ?units .
            FILTER(?units IN (punits:month, punits:day, punits:hour))
          }
          UNION
          {
            # Option 3: No units specified (assume years)
            ?resolution le:hasMaxValue ?maxValue .
            FILTER NOT EXISTS { ?resolution le:hasUnits ?anyUnits }
            FILTER(?maxValue <= 10)
          }
        """

    def build_time_period_filter(self, period_name: str, overlap_type: str = "partial") -> str:
        """
        Build a SPARQL filter for a time period.

        Args:
            period_name (str): The name of the time period (e.g., "Holocene", "LGM")
            overlap_type (str): Type of overlap between dataset and time period:
                - "partial": Dataset time range overlaps with any part of the time period (default)
                - "containing": Dataset time range fully contains the time period
                - "contained": Dataset time range is fully contained within the time period

        Returns:
            str: SPARQL query fragment for the time period filter
        """
        # Define time periods with their BP year ranges (years before 1950 CE)
        time_periods = {
            # Recent and historical periods
            "lastcentury": (0, 100),              # Last century
            "Industrial": (0, 170),               # ~1780 CE to present
            "LastMillennium": (0, 1000),          # 1000 years BP to present
            "CommonEra": (0, 2000),               # 2000 years BP to present (roughly 0 CE to present)
            
            # Holocene subdivisions
            "LateHolocene": (0, 4200),            # 4.2ka BP to present
            "MidHolocene": (4200, 8200),          # 8.2-4.2ka BP
            "EarlyHolocene": (8200, 11700),       # 11.7-8.2ka BP
            "Holocene": (0, 11700),               # 11.7ka BP to present
            
            # Late Pleistocene events
            "YoungerDryas": (11700, 12900),       # 12.9-11.7ka BP
            "BøllingAllerød": (12900, 14700),     # 14.7-12.9ka BP
            "LastGlacialMaximum": (19000, 26500), # 26.5-19ka BP
            "LastGlacial": (11700, 115000),       # 115-11.7ka BP
            "LastInterglacial": (115000, 130000), # 130-115ka BP
            
            # Standard Geological Time Scale periods
            "Pleistocene": (11700, 2580000),      # 2.58 Ma to 11.7 ka BP
            "Quaternary": (0, 2580000),           # 2.58 Ma to present
            "Gelasian": (1800000, 2580000),       # 2.58-1.8 Ma BP
            "Calabrian": (781000, 1800000),       # 1.8-0.781 Ma BP
            "MiddlePleistocene": (126000, 781000), # 781-126 ka BP
            "LatePleistocene": (11700, 126000),   # 126-11.7 ka BP
            
            # Neogene
            "Pliocene": (2580000, 5333000),       # 5.333-2.58 Ma BP
            "Zanclean": (3600000, 5333000),       # 5.333-3.6 Ma BP
            "Piacenzian": (2580000, 3600000),     # 3.6-2.58 Ma BP
            "Miocene": (5333000, 23030000),       # 23.03-5.333 Ma BP
            "EarlyMiocene": (15970000, 23030000), # 23.03-15.97 Ma BP
            "MiddleMiocene": (11630000, 15970000), # 15.97-11.63 Ma BP
            "LateMiocene": (5333000, 11630000),   # 11.63-5.333 Ma BP
            "Neogene": (2580000, 23030000),       # 23.03-2.58 Ma BP
            
            # Paleogene
            "Oligocene": (23030000, 33900000),    # 33.9-23.03 Ma BP
            "Rupelian": (28100000, 33900000),     # 33.9-28.1 Ma BP
            "Chattian": (23030000, 28100000),     # 28.1-23.03 Ma BP
            "Eocene": (33900000, 56000000),       # 56-33.9 Ma BP
            "EarlyEocene": (47800000, 56000000),  # 56-47.8 Ma BP
            "MiddleEocene": (38000000, 47800000), # 47.8-38 Ma BP
            "LateEocene": (33900000, 38000000),   # 38-33.9 Ma BP
            "Paleocene": (56000000, 66000000),    # 66-56 Ma BP
            "Paleogene": (23030000, 66000000),    # 66-23.03 Ma BP
            
            # Cenozoic Era
            "Cenozoic": (0, 66000000),            # 66 Ma to present
            
            # Mesozoic Era and its periods
            "Cretaceous": (66000000, 145000000),  # 145-66 Ma BP
            "EarlyCretaceous": (100500000, 145000000), # 145-100.5 Ma BP
            "LateCretaceous": (66000000, 100500000),   # 100.5-66 Ma BP
            "Jurassic": (145000000, 201300000),   # 201.3-145 Ma BP
            "EarlyJurassic": (174100000, 201300000),   # 201.3-174.1 Ma BP
            "MiddleJurassic": (163500000, 174100000),  # 174.1-163.5 Ma BP
            "LateJurassic": (145000000, 163500000),    # 163.5-145 Ma BP
            "Triassic": (201300000, 251902000),   # 251.902-201.3 Ma BP
            "EarlyTriassic": (247200000, 251902000),   # 251.902-247.2 Ma BP
            "MiddleTriassic": (237000000, 247200000),  # 247.2-237 Ma BP
            "LateTriassic": (201300000, 237000000),    # 237-201.3 Ma BP
            "Mesozoic": (66000000, 251902000),    # 251.902-66 Ma BP
            
            # Paleozoic Era and its periods
            "Permian": (251902000, 298900000),    # 298.9-251.902 Ma BP
            "Carboniferous": (298900000, 358900000), # 358.9-298.9 Ma BP
            "Pennsylvanian": (298900000, 323200000), # 323.2-298.9 Ma BP
            "Mississippian": (323200000, 358900000), # 358.9-323.2 Ma BP
            "Devonian": (358900000, 419200000),   # 419.2-358.9 Ma BP
            "Silurian": (419200000, 443800000),   # 443.8-419.2 Ma BP
            "Ordovician": (443800000, 485400000), # 485.4-443.8 Ma BP
            "Cambrian": (485400000, 538800000),   # 538.8-485.4 Ma BP
            "Paleozoic": (251902000, 538800000),  # 538.8-251.902 Ma BP
            
            # Precambrian (informal)
            "Ediacaran": (538800000, 635000000),  # 635-538.8 Ma BP
            "Cryogenian": (635000000, 720000000), # 720-635 Ma BP
            "Tonian": (720000000, 1000000000),    # 1000-720 Ma BP
            "Neoproterozoic": (538800000, 1000000000), # 1000-538.8 Ma BP
            "Mesoproterozoic": (1000000000, 1600000000), # 1600-1000 Ma BP
            "Paleoproterozoic": (1600000000, 2500000000), # 2500-1600 Ma BP
            "Proterozoic": (538800000, 2500000000), # 2500-538.8 Ma BP
            "Archean": (2500000000, 4000000000),  # 4000-2500 Ma BP
            "Hadean": (4000000000, 4600000000),   # 4600-4000 Ma BP
            "Precambrian": (538800000, 4600000000) # 4600-538.8 Ma BP
        }
        
        # Check if the period name is valid
        if period_name not in time_periods:
            logger.warning(f"Unknown time period: {period_name}. No SPARQL filtering will be applied.")
            return ""
        
        # Validate overlap_type
        valid_overlap_types = ["partial", "containing", "contained"]
        if overlap_type not in valid_overlap_types:
            logger.warning(f"Unknown overlap type: {overlap_type}. Using 'partial' as default.")
            overlap_type = "partial"
            
        # Get the year range for the requested time period
        min_year_bp, max_year_bp = time_periods[period_name]
        
        # Create a unique suffix for the variables in the query
        suffix = period_name.lower()
        
        # Build the SPARQL query fragment to find datasets with time variables that overlap with the period
        query = f"""
          # Find datasets with time variables that may fall within the {period_name} period
          ?dataset le:hasPaleoData ?paleoData_time_{suffix} .
          ?paleoData_time_{suffix} le:hasMeasurementTable ?dataTable_time_{suffix} .
          ?dataTable_time_{suffix} le:hasVariable ?variable_time_{suffix} .
          
          # Look for variables that represent time/age
          {{
            # Option 1: Variables with standard time/age variables
            ?variable_time_{suffix} le:hasStandardVariable ?stdVar_{suffix} .
            FILTER(?stdVar_{suffix} IN (pvar:age, pvar:year))
          }}
          UNION
          {{
            # Option 2: Variables with names indicating time
            ?variable_time_{suffix} rdfs:label ?varLabel_{suffix} .
            FILTER(REGEX(?varLabel_{suffix}, "year|age|yr|ka", "i"))
          }}
          
          # Check variable values
          ?variable_time_{suffix} le:hasMinValue ?min_time_{suffix} ;
                          le:hasMaxValue ?max_time_{suffix} .
          
          # Get units for conversion
          ?variable_time_{suffix} le:hasUnits ?units_time_{suffix} .
          
          # Filter based on units and value ranges
          {{
            # Case 1: yr_BP with direct comparison
            FILTER(?units_time_{suffix} IN (punits:yr_BP))
        """
        
        # Add the appropriate filter condition based on the overlap type
        if overlap_type == "partial":
            # Partial overlap - dataset time range overlaps with any part of the period
            query += f"""
            # Check if time range overlaps with our period (partial overlap)
            FILTER(
              (?min_time_{suffix} <= {max_year_bp} && ?max_time_{suffix} >= {min_year_bp})
            )
            """
        elif overlap_type == "containing":
            # Containing - dataset time range fully contains the period
            query += f"""
            # Check if time range contains our period (dataset fully contains the period)
            FILTER(
              (?min_time_{suffix} <= {min_year_bp} && ?max_time_{suffix} >= {max_year_bp})
            )
            """
        elif overlap_type == "contained":
            # Contained - dataset time range is fully contained within the period
            query += f"""
            # Check if time range is contained within our period (dataset fully contained in period)
            FILTER(
              (?min_time_{suffix} >= {min_year_bp} && ?max_time_{suffix} <= {max_year_bp})
            )
            """
            
        query += f"""
          }}
          UNION
          {{
            # Case 2: yr_CE/AD needs conversion (1950 - yr_CE = yr_BP)
            FILTER(?units_time_{suffix} IN (punits:yr_AD, punits:yr_CE))
            # Convert yr_CE to yr_BP: 1950 - yr_CE = yr_BP
            BIND(1950 - ?max_time_{suffix} AS ?min_bp_{suffix})
            BIND(1950 - ?min_time_{suffix} AS ?max_bp_{suffix})
        """
        
        # Add the appropriate filter condition based on the overlap type
        if overlap_type == "partial":
            # Partial overlap
            query += f"""
            # Check if time range overlaps with our period (partial overlap)
            FILTER(
              (?min_bp_{suffix} <= {max_year_bp} && ?max_bp_{suffix} >= {min_year_bp})
            )
            """
        elif overlap_type == "containing":
            # Containing - dataset time range fully contains the period
            query += f"""
            # Check if time range contains our period (dataset fully contains the period)
            FILTER(
              (?min_bp_{suffix} <= {min_year_bp} && ?max_bp_{suffix} >= {max_year_bp})
            )
            """
        elif overlap_type == "contained":
            # Contained - dataset time range is fully contained within the period
            query += f"""
            # Check if time range is contained within our period (dataset fully contained in period)
            FILTER(
              (?min_bp_{suffix} >= {min_year_bp} && ?max_bp_{suffix} <= {max_year_bp})
            )
            """
            
        query += f"""
          }}
          UNION
          {{
            # Case 3: yr_b2k needs conversion (yr_b2k - 50 = yr_BP)
            FILTER(?units_time_{suffix} IN (punits:yr_b2k))
            # Convert yr_b2k to yr_BP: yr_b2k - 50 = yr_BP
            BIND(?min_time_{suffix} - 50 AS ?min_bp_{suffix})
            BIND(?max_time_{suffix} - 50 AS ?max_bp_{suffix})
        """
        
        # Add the appropriate filter condition based on the overlap type
        if overlap_type == "partial":
            # Partial overlap
            query += f"""
            # Check if time range overlaps with our period (partial overlap)
            FILTER(
              (?min_bp_{suffix} <= {max_year_bp} && ?max_bp_{suffix} >= {min_year_bp})
            )
            """
        elif overlap_type == "containing":
            # Containing - dataset time range fully contains the period
            query += f"""
            # Check if time range contains our period (dataset fully contains the period)
            FILTER(
              (?min_bp_{suffix} <= {min_year_bp} && ?max_bp_{suffix} >= {max_year_bp})
            )
            """
        elif overlap_type == "contained":
            # Contained - dataset time range is fully contained within the period
            query += f"""
            # Check if time range is contained within our period (dataset fully contained in period)
            FILTER(
              (?min_bp_{suffix} >= {min_year_bp} && ?max_bp_{suffix} <= {max_year_bp})
            )
            """
            
        query += f"""
          }}
          UNION
          {{
            # Case 4: ka_BP needs conversion (ka_BP * 1000 = yr_BP)
            FILTER(?units_time_{suffix} IN (punits:ka, punits:kyr))
            # Convert ka_BP to yr_BP: ka_BP * 1000 = yr_BP
            BIND(?min_time_{suffix} * 1000 AS ?min_bp_{suffix})
            BIND(?max_time_{suffix} * 1000 AS ?max_bp_{suffix})
        """
        
        # Add the appropriate filter condition based on the overlap type
        if overlap_type == "partial":
            # Partial overlap
            query += f"""
            # Check if time range overlaps with our period (partial overlap)
            FILTER(
              (?min_bp_{suffix} <= {max_year_bp} && ?max_bp_{suffix} >= {min_year_bp})
            )
            """
        elif overlap_type == "containing":
            # Containing - dataset time range fully contains the period
            query += f"""
            # Check if time range contains our period (dataset fully contains the period)
            FILTER(
              (?min_bp_{suffix} <= {min_year_bp} && ?max_bp_{suffix} >= {max_year_bp})
            )
            """
        elif overlap_type == "contained":
            # Contained - dataset time range is fully contained within the period
            query += f"""
            # Check if time range is contained within our period (dataset fully contained in period)
            FILTER(
              (?min_bp_{suffix} >= {min_year_bp} && ?max_bp_{suffix} <= {max_year_bp})
            )
            """
            
        query += f"""
          }}
        """
        
        return query

    def build_location_filter(self, region_name: str) -> str:
        """
        Build a SPARQL filter for a geographic region.

        Args:
            region_name (str): The name of the region (e.g., "North America", "Pacific Ocean")

        Returns:
            str: SPARQL query fragment for the location filter
        """
        # Dictionary of region names and their bounding boxes (min_lat, max_lat, min_lon, max_lon)
        # Coordinates are in decimal degrees, with negative values for south latitude and west longitude
        regions = {
            # Continents
            "North America": (10.0, 72.0, -170.0, -50.0),
            "South America": (-56.0, 15.0, -90.0, -30.0),
            "Europe": (36.0, 72.0, -10.0, 40.0),
            "Asia": (0.0, 80.0, 40.0, 180.0),
            "Africa": (-35.0, 37.0, -20.0, 52.0),
            "Australia": (-45.0, -10.0, 110.0, 155.0),
            "Antarctica": (-90.0, -60.0, -180.0, 180.0),
            
            # Oceans
            "Pacific Ocean": (-60.0, 65.0, 120.0, -70.0),  # Crosses the date line
            "Atlantic Ocean": (-70.0, 65.0, -80.0, 20.0),
            "Indian Ocean": (-70.0, 30.0, 20.0, 120.0),
            "Arctic Ocean": (65.0, 90.0, -180.0, 180.0),
            "Southern Ocean": (-90.0, -60.0, -180.0, 180.0),
            
            # Specific regions
            "Mediterranean": (30.0, 46.0, -5.0, 36.0),
            "Caribbean": (8.0, 28.0, -90.0, -60.0),
            "Middle East": (12.0, 42.0, 24.0, 63.0),
            "Southeast Asia": (-10.0, 30.0, 90.0, 150.0),
            
            # Climate zones
            "Tropics": (-23.5, 23.5, -180.0, 180.0),
            "Arctic": (66.5, 90.0, -180.0, 180.0),
            "Antarctic": (-90.0, -66.5, -180.0, 180.0),
            
            # ENSO regions
            "Niño 3.4": (-5.0, 5.0, -170.0, -120.0),
            "Niño 3": (-5.0, 5.0, -150.0, -90.0),
            "Niño 4": (-5.0, 5.0, 160.0, -150.0),
            "Niño 1+2": (-10.0, 0.0, -90.0, -80.0),
            
            # Other common regions
            "Tropical Pacific": (-23.5, 23.5, 120.0, -70.0),  # Crosses the date line
            "North Atlantic": (30.0, 65.0, -80.0, 0.0),
            "South Atlantic": (-60.0, 0.0, -70.0, 20.0),
            "North Pacific": (30.0, 65.0, 120.0, -100.0),  # Crosses the date line
            "South Pacific": (-60.0, 0.0, 150.0, -70.0),   # Crosses the date line
            "Western Europe": (36.0, 72.0, -10.0, 20.0),
            "Eastern Europe": (36.0, 72.0, 20.0, 40.0),
            "East Asia": (10.0, 50.0, 100.0, 145.0),
            "South Asia": (5.0, 40.0, 60.0, 100.0),
            "Central America": (7.0, 33.0, -120.0, -60.0),
            "Greenland": (60.0, 85.0, -75.0, -10.0),
            "Amazon Basin": (-20.0, 5.0, -80.0, -45.0),
            "Sahara": (15.0, 35.0, -15.0, 35.0),
            "Tibetan Plateau": (25.0, 40.0, 70.0, 105.0),
            "Himalayas": (25.0, 40.0, 70.0, 95.0),
        }
        
        # Normalize region name for case-insensitive matching
        normalized_region = region_name.lower()
        
        # Find the matching region
        bounding_box = None
        for region, bbox in regions.items():
            if region.lower() == normalized_region:
                bounding_box = bbox
                break
            
        # If no exact match, try partial matching
        if bounding_box is None:
            for region, bbox in regions.items():
                if normalized_region in region.lower() or region.lower() in normalized_region:
                    bounding_box = bbox
                    logger.info(f"Using partial match: '{region}' for region query '{region_name}'")
                    break
        
        # If still no match, return empty string
        if bounding_box is None:
            logger.warning(f"Unknown region: {region_name}. No location filtering will be applied.")
            return ""
        
        # Extract bounding box coordinates
        min_lat, max_lat, min_lon, max_lon = bounding_box
        
        # Create a unique suffix for the variables in the query
        suffix = region_name.replace(" ", "_").lower()
        
        # Handle regions that cross the international date line (where min_lon > max_lon)
        date_line_crossing = min_lon > max_lon
        
        # Build the SPARQL query fragment
        query = f"""
          # Find datasets with locations in the {region_name} region
          ?dataset le:hasLocation ?location_{suffix} .
          
          # Get location coordinates
          ?location_{suffix} le:hasLatitude ?lat_{suffix} ;
                      le:hasLongitude ?lon_{suffix} .
          
          # Filter coordinates within the bounding box
          FILTER(?lat_{suffix} >= {min_lat} && ?lat_{suffix} <= {max_lat})
        """
        
        # Special handling for regions that cross the international date line
        if date_line_crossing:
            query += f"""
          # Region crosses the international date line, so use OR condition for longitude
          FILTER(?lon_{suffix} >= {min_lon} || ?lon_{suffix} <= {max_lon})
            """
        else:
            query += f"""
          FILTER(?lon_{suffix} >= {min_lon} && ?lon_{suffix} <= {max_lon})
            """
        
        return query

    def build_custom_sparql_query(
        self,
        archive_type: Optional[str] = None, 
        variables: Optional[List[str]] = None,
        interpretations: Optional[List[str]] = None,
        resolution: Optional[str] = None,
        time_period: Optional[str] = None,
        location: Optional[str] = None,
        time_overlap: Optional[str] = "partial"
    ) -> str:
        """
        Build a custom SPARQL query based on the provided parameters.

        Args:
            archive_type (str, optional): The archive type to filter for.
            variables (List[str], optional): List of variables to filter for.
            interpretations (List[str], optional): List of interpretation variables.
            resolution (str, optional): Resolution type to filter for.
            time_period (str, optional): Time period to filter for.
            location (str, optional): Region to filter for.
            time_overlap (str, optional): Type of time period overlap ("partial", "containing", "contained").

        Returns:
            str: Complete SPARQL query
        """
        # Start with the prefixes
        query_parts = [self.build_prefixes()]
        
        # SELECT clause
        query_parts.append("SELECT DISTINCT ?datasetName WHERE {")
        
        # Add archive type filter
        if archive_type:
            query_parts.append(self.build_archive_filter(archive_type))
        else:
            # If no archive type specified, still need to get the dataset name
            query_parts.append("  ?dataset rdf:type le:Dataset .")
            query_parts.append("  ?dataset le:hasName ?datasetName .")
        
        # Add variable filters
        if variables:
            for variable in variables:
                query_parts.append(self.build_variable_filter("pvar", variable))
        
        # Add interpretation filters
        if interpretations:
            interpretation_parts = []
            for interp in interpretations:
                interpretation_parts.append(self.build_interpretation_filter(interp))
            
            if variables:  # If we have both variables and interpretations, we need a UNION
                query_parts.append("  {")
                # If we have direct variables, those go in the first part of the UNION
                query_parts.append("    # Direct variables already added above")
                query_parts.append("  }")
                query_parts.append("  UNION")
                query_parts.append("  {")
                # Interpretation filters go in the second part
                query_parts.extend(interpretation_parts)
                query_parts.append("  }")
            else:
                # If no direct variables, just add the interpretation filters
                query_parts.extend(interpretation_parts)
        
        # Add resolution filter if specified
        if resolution:
            if variables:
                # Use the first variable for resolution filtering
                var_suffix = variables[0].replace("/", "_").lower()
                res_filter = self.build_resolution_filter(resolution)
                # Replace the generic variable_res with the specific variable from our first filter
                res_filter = res_filter.replace("?variable_res", f"?variable_{var_suffix}")
                query_parts.append(res_filter)
            elif interpretations:
                # Use the first interpretation for resolution filtering
                interp_suffix = interpretations[0].replace("/", "_").lower()
                res_filter = self.build_resolution_filter(resolution)
                # Replace the generic variable_res with the specific interpretation variable
                res_filter = res_filter.replace("?variable_res", f"?variable_interp_{interp_suffix}")
                query_parts.append(res_filter)
        
        # Add time period filter if specified
        if time_period:
            query_parts.append(self.build_time_period_filter(time_period, time_overlap))
        
        # Add location filter if specified
        if location:
            query_parts.append(self.build_location_filter(location))
        
        # Close the query
        query_parts.append("}")
        
        # Join all parts with newlines and return
        query = "\n".join(query_parts)
        logger.debug(f"Generated SPARQL query:\n{query}")
        return query

    def execute_sparql_query(self, query: str) -> List[str]:
        """
        Execute a SPARQL query and return the results.

        Args:
            query (str): The SPARQL query to execute

        Returns:
            List[str]: List of dataset names from the query results
        """
        logger.info("Executing SPARQL query...")
        logger.debug(f"Query: {query}")
        
        try:
            self.sparql.setQuery(query)
            results = self.sparql.query().convert()
            
            dataset_names = []
            for result in results["results"]["bindings"]:
                dataset_names.append(result["datasetName"]["value"])
            
            logger.info(f"Found {len(dataset_names)} potential datasets in GraphDB.")
            return dataset_names
        
        except Exception as e:
            logger.error(f"SPARQL query failed: {str(e)}")
            return []


    def process_query(
        self,
        archive_type: Optional[str] = None,
        variables: Optional[List[str]] = None,
        interpretations: Optional[List[str]] = None,
        resolution: Optional[str] = None,
        time_period: Optional[str] = None,
        location: Optional[str] = None,
        time_overlap: Optional[str] = "partial"
    ) -> List[str]:
        """
        Process a query to find dataset names matching the criteria.

        Args:
            archive_type (str, optional): Archive type to filter for.
            variables (List[str], optional): List of variables to filter for.
            interpretations (List[str], optional): List of interpretation variables.
            resolution (str, optional): Resolution term to filter for.
            time_period (str, optional): Time period to filter for.
            location (str, optional): Region to filter for.
            time_overlap (str, optional): Type of time period overlap ("partial", "containing", "contained").

        Returns:
            List[str]: List of dataset names matching the criteria
        """
        # Test the connection first
        if not self.test_connection():
            logger.error("Connection to GraphDB failed. Aborting query.")
            return []
        
        # Build and execute the SPARQL query
        query = self.build_custom_sparql_query(
            archive_type=archive_type,
            variables=variables,
            interpretations=interpretations,
            resolution=resolution,
            time_period=time_period,
            location=location,
            time_overlap=time_overlap
        )
        
        dataset_names = self.execute_sparql_query(query)
        
        if not dataset_names:
            logger.warning("No datasets found matching the criteria in GraphDB.")
            return []
        
        logger.info(f"Found {len(dataset_names)} datasets matching the criteria.")
        return dataset_names


def main():
    """Main function with command-line argument parsing."""
    parser = argparse.ArgumentParser(description="Query paleoclimate data from GraphDB and LiPD.")
    
    # GraphDB connection parameters
    parser.add_argument("--endpoint", type=str, default="https://linkedearth.graphdb.mint.isi.edu",
                       help="GraphDB endpoint URL (default: https://linkedearth.graphdb.mint.isi.edu)")
    parser.add_argument("--repository", type=str, default="LiPDVerse-dynamic",
                       help="GraphDB repository name (default: LiPDVerse-dynamic)")
    
    # Query parameters
    parser.add_argument("--archive", type=str,
                       help="Archive type to filter for (e.g., Coral, GlacierIce)")
    parser.add_argument("--variables", type=str, nargs="+",
                       help="Variables to filter for (e.g., d18O temperature)")
    parser.add_argument("--interpretations", type=str, nargs="+",
                       help="Interpretation variables to filter for (e.g., temperature precipitation)")
    parser.add_argument("--resolution", type=str,
                       help="Resolution term to filter for (e.g., annual|yr, decadal)")
    parser.add_argument("--time-period", type=str,
                       help="Time period to filter for (e.g., Holocene, LGM, CommonEra)")
    parser.add_argument("--time-overlap", type=str, default="partial", choices=["partial", "containing", "contained"],
                       help="Type of time period overlap (partial: dataset overlaps time period; containing: dataset "
                            "contains the time period; contained: dataset is contained within the time period)")
    parser.add_argument("--location", type=str,
                       help="Region to filter for (e.g., North America, Pacific Ocean)")
    
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
        # Also set the handler levels
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    # Create query object
    query = DataQuery(args.endpoint, args.repository)
    
    # Process query
    dataset_names = query.process_query(
        archive_type=args.archive,
        variables=args.variables,
        interpretations=args.interpretations,
        resolution=args.resolution,
        time_period=args.time_period,
        location=args.location,
        time_overlap=args.time_overlap
    )
    
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