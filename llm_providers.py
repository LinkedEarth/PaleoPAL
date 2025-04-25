#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLM Providers for PaleoPal

This module provides a flexible abstraction for connecting to various
Language Learning Models (LLMs) including Ollama, OpenAI, Claude, etc.
"""

import abc
import json
import logging
import os
from typing import Dict, List, Optional, Any, Union

# Import providers (with conditional imports to handle missing dependencies)
try:
    import ollama
except ImportError:
    ollama = None

try:
    import openai
except ImportError:
    openai = None

try:
    import anthropic
except ImportError:
    anthropic = None

# Configure logging
logger = logging.getLogger(__name__)


class LLMProvider(abc.ABC):
    """
    Abstract base class for LLM providers.
    All LLM provider implementations should inherit from this class.
    """
    
    @abc.abstractmethod
    def generate_response(self, prompt: str) -> str:
        """
        Generate a response from the LLM based on the given prompt.
        
        Args:
            prompt (str): The prompt to send to the LLM
            
        Returns:
            str: The LLM's response
        """
        pass
    
    @abc.abstractmethod
    def is_available(self) -> bool:
        """
        Check if this provider is available for use.
        
        Returns:
            bool: True if the provider is available, False otherwise
        """
        pass


class OllamaProvider(LLMProvider):
    """
    Provider for Ollama LLMs (local models like Llama2, DeepSeek, etc.)
    """
    
    def __init__(self, model_name: str = "deepseek-r1"):
        """
        Initialize the Ollama provider.
        
        Args:
            model_name (str): The name of the Ollama model to use
        """
        self.model_name = model_name
    
    def is_available(self) -> bool:
        """
        Check if Ollama is available.
        
        Returns:
            bool: True if Ollama is available, False otherwise
        """
        if ollama is None:
            logger.warning("Ollama package is not installed. Install with: pip install ollama")
            return False
        
        try:
            # Try to list models to check if Ollama is running
            _ = ollama.list()
            return True
        except Exception as e:
            logger.warning(f"Ollama is not available: {str(e)}")
            return False
    
    def generate_response(self, prompt: str) -> str:
        """
        Generate a response using Ollama.
        
        Args:
            prompt (str): The prompt to send to Ollama
            
        Returns:
            str: The response from Ollama
        """
        if not self.is_available():
            raise RuntimeError("Ollama is not available")
        
        try:
            response = ollama.chat(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}]
            )
            return response['message']['content']
        except Exception as e:
            logger.error(f"Error calling Ollama: {str(e)}")
            raise


class OpenAIProvider(LLMProvider):
    """
    Provider for OpenAI's models (GPT-3.5, GPT-4, etc.)
    """
    
    def __init__(
        self,
        model_name: str = "gpt-3.5-turbo",
        api_key: Optional[str] = None,
        temperature: float = 0.0
    ):
        """
        Initialize the OpenAI provider.
        
        Args:
            model_name (str): The name of the OpenAI model to use
            api_key (str, optional): The OpenAI API key. If None, will use OPENAI_API_KEY env var.
            temperature (float): Temperature setting for generation (0.0 = deterministic)
        """
        self.model_name = model_name
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.temperature = temperature
    
    def is_available(self) -> bool:
        """
        Check if OpenAI is available.
        
        Returns:
            bool: True if OpenAI is available, False otherwise
        """
        if openai is None:
            logger.warning("OpenAI package is not installed. Install with: pip install openai")
            return False
        
        if not self.api_key:
            logger.warning("OpenAI API key is not set")
            return False
        
        return True
    
    def generate_response(self, prompt: str) -> str:
        """
        Generate a response using OpenAI.
        
        Args:
            prompt (str): The prompt to send to OpenAI
            
        Returns:
            str: The response from OpenAI
        """
        if not self.is_available():
            raise RuntimeError("OpenAI is not available")
        
        client = openai.OpenAI(api_key=self.api_key)
        
        try:
            response = client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error calling OpenAI: {str(e)}")
            raise


class ClaudeProvider(LLMProvider):
    """
    Provider for Anthropic's Claude models
    """
    
    def __init__(
        self,
        model_name: str = "claude-3-opus-20240229",
        api_key: Optional[str] = None,
        temperature: float = 0.0
    ):
        """
        Initialize the Claude provider.
        
        Args:
            model_name (str): The name of the Claude model to use
            api_key (str, optional): The Anthropic API key. If None, will use ANTHROPIC_API_KEY env var.
            temperature (float): Temperature setting for generation (0.0 = deterministic)
        """
        self.model_name = model_name
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.temperature = temperature
    
    def is_available(self) -> bool:
        """
        Check if Claude is available.
        
        Returns:
            bool: True if Claude is available, False otherwise
        """
        if anthropic is None:
            logger.warning("Anthropic package is not installed. Install with: pip install anthropic")
            return False
        
        if not self.api_key:
            logger.warning("Anthropic API key is not set")
            return False
        
        return True
    
    def generate_response(self, prompt: str) -> str:
        """
        Generate a response using Claude.
        
        Args:
            prompt (str): The prompt to send to Claude
            
        Returns:
            str: The response from Claude
        """
        if not self.is_available():
            raise RuntimeError("Claude is not available")
        
        client = anthropic.Anthropic(api_key=self.api_key)
        
        try:
            response = client.messages.create(
                model=self.model_name,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Error calling Claude: {str(e)}")
            raise


class LLMProviderFactory:
    """
    Factory for creating LLM providers.
    """
    
    @staticmethod
    def create_provider(
        provider_type: str,
        model_name: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 0.0
    ) -> LLMProvider:
        """
        Create an LLM provider of the specified type.
        
        Args:
            provider_type (str): The type of provider to create 
                                ('ollama', 'openai', 'claude')
            model_name (str, optional): The name of the model to use
            api_key (str, optional): The API key for the provider
            temperature (float): Temperature setting for generation
            
        Returns:
            LLMProvider: An instance of the specified LLM provider
            
        Raises:
            ValueError: If the provider type is not recognized
        """
        provider_type = provider_type.lower()
        
        if provider_type == "ollama":
            default_model = "deepseek-r1"
            return OllamaProvider(model_name=model_name or default_model)
        
        elif provider_type == "openai":
            default_model = "gpt-3.5-turbo"
            return OpenAIProvider(
                model_name=model_name or default_model,
                api_key=api_key,
                temperature=temperature
            )
        
        elif provider_type == "claude":
            default_model = "claude-3-opus-20240229"
            return ClaudeProvider(
                model_name=model_name or default_model,
                api_key=api_key,
                temperature=temperature
            )
        
        else:
            raise ValueError(f"Unknown LLM provider type: {provider_type}")
    
    @staticmethod
    def get_available_provider(
        preferred_providers: List[str] = ["ollama", "openai", "claude"],
        model_name: Optional[str] = None,
        api_key: Optional[str] = None
    ) -> Optional[LLMProvider]:
        """
        Get the first available LLM provider from the preferred list.
        
        Args:
            preferred_providers (List[str]): List of provider types in order of preference
            model_name (str, optional): The name of the model to use
            api_key (str, optional): The API key for the provider
            
        Returns:
            LLMProvider or None: An instance of the first available LLM provider, or None if none are available
        """
        for provider_type in preferred_providers:
            try:
                provider = LLMProviderFactory.create_provider(
                    provider_type=provider_type,
                    model_name=model_name,
                    api_key=api_key
                )
                if provider.is_available():
                    logger.info(f"Using LLM provider: {provider_type}")
                    return provider
            except Exception as e:
                logger.warning(f"Error creating provider {provider_type}: {str(e)}")
        
        logger.error("No available LLM providers found")
        return None 