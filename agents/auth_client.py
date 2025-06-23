import os
import requests
import time
import logging
import json

# Attempt to get CTI_BACKEND_URL from environment, with a fallback for local dev if needed
CTI_BACKEND_URL = os.getenv("CTI_BACKEND_URL", "http://localhost:8000/api") 
# AGENT_TOKEN_ENDPOINT will be constructed using CTI_BACKEND_URL and a path from config or default
AGENT_TOKEN_ENDPOINT_PATH = os.getenv("AGENT_TOKEN_ENDPOINT_PATH", "/v1/agent/auth/token")
AGENT_TOKEN_ENDPOINT = f"{CTI_BACKEND_URL}{AGENT_TOKEN_ENDPOINT_PATH}"

CTI_API_KEY = os.getenv("CTI_API_KEY")

_current_jwt = None
_jwt_expires_at = 0  # Unix timestamp
_TOKEN_REFRESH_BUFFER_SECONDS = 300  # Refresh 5 minutes before expiry

logger = logging.getLogger(__name__)

def get_agent_jwt():
    """
    Retrieves a short-lived JWT for the agent.
    Uses the static CTI_API_KEY for initial authentication to the token endpoint.
    Manages token refresh automatically.
    """
    global _current_jwt, _jwt_expires_at

    if not CTI_API_KEY:
        logger.error("CTI_API_KEY is not set. Agent cannot authenticate.")
        raise ValueError("CTI_API_KEY not configured for agent authentication.")

    current_time = time.time()
    if _current_jwt and current_time < (_jwt_expires_at - _TOKEN_REFRESH_BUFFER_SECONDS):
        logger.debug("Using existing agent JWT.")
        return _current_jwt

    logger.info(f"Acquiring new agent JWT from {AGENT_TOKEN_ENDPOINT}...")
    try:
        headers = {
            "X-Agent-Bootstrap-Key": CTI_API_KEY,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Make the request to the agent token endpoint
        # A payload can be added if your token endpoint expects specific agent identifiers
        # payload = {"agent_id": os.getenv("AGENT_NAME", "generic_agent")} 
        response = requests.post(AGENT_TOKEN_ENDPOINT, headers=headers) #, json=payload if payload else None)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

        token_data = response.json()
        
        _current_jwt = token_data.get("access_token")
        expires_in = token_data.get("expires_in") # Expected: lifetime in seconds

        if not _current_jwt:
            logger.error("Access token not found in response from auth endpoint.")
            raise ValueError("Access token not found in response")
        
        if expires_in:
            _jwt_expires_at = current_time + int(expires_in)
        else: # Fallback if 'expires_in' is not provided, not recommended for production
            logger.warning("Token 'expires_in' not found in response. Defaulting to 1 hour.")
            _jwt_expires_at = current_time + 3600 
            
        logger.info(f"Successfully acquired new agent JWT. Expires at: {time.ctime(_jwt_expires_at)}")
        return _current_jwt

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error acquiring agent JWT: {http_err} - Response: {http_err.response.text}")
        _current_jwt = None 
        _jwt_expires_at = 0
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error acquiring agent JWT: {req_err}")
        _current_jwt = None
        _jwt_expires_at = 0
        raise
    except (ValueError, KeyError) as val_err: # Handles JSON parsing errors or missing keys
        logger.error(f"Error processing token response: {val_err}")
        _current_jwt = None
        _jwt_expires_at = 0
        raise

class AgentHttpClient:
    """
    A wrapper around requests.Session that automatically handles
    agent JWT authentication and refresh.
    """
    def __init__(self):
        self.session = requests.Session()

    def _get_auth_headers(self):
        jwt_token = get_agent_jwt()
        return {"Authorization": f"Bearer {jwt_token}", "Accept": "application/json"}

    def request(self, method, url, **kwargs):
        """Makes an authenticated request to the backend."""
        auth_headers = self._get_auth_headers()
        
        headers = kwargs.pop("headers", {})
        headers.update(auth_headers)
        
        # Ensure the full URL is constructed if a relative path is given
        if not url.startswith(('http://', 'https://')):
            full_url = f"{CTI_BACKEND_URL}{url}" # Assumes url might be a relative path from /api
        else:
            full_url = url
            
        logger.debug(f"AgentHttpClient making {method.upper()} request to {full_url}")
        try:
            response = self.session.request(method, full_url, headers=headers, **kwargs)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"API Request HTTP Error: {http_err} for {method.upper()} {full_url} - Response: {http_err.response.text}")
            raise
        except requests.exceptions.RequestException as req_err:
            logger.error(f"API Request Error: {req_err} for {method.upper()} {full_url}")
            raise

    def get(self, url, **kwargs):
        return self.request("get", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("post", url, **kwargs)

    def put(self, url, **kwargs):
        return self.request("put", url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("delete", url, **kwargs)

# Example of how an agent might use it:
if __name__ == '__main__':
    # This is for testing the auth_client.py itself.
    # Ensure CTI_API_KEY and CTI_BACKEND_URL are set in your environment.
    # You would also need a running backend with the /v1/agent/auth/token endpoint.
    
    logging.basicConfig(level=logging.INFO)
    logger.info("Testing AgentHttpClient...")
    
    if not CTI_API_KEY:
        logger.error("Please set CTI_API_KEY environment variable for testing.")
    else:
        try:
            # Test getting JWT
            jwt = get_agent_jwt()
            logger.info(f"Test: Acquired JWT successfully: {jwt[:20]}...") # Print first 20 chars

            # Test making a GET request (replace with an actual endpoint on your backend)
            # client = AgentHttpClient()
            # test_endpoint = "/v1/system/health" # Example endpoint
            # logger.info(f"Test: Making GET request to {test_endpoint}...")
            # response = client.get(test_endpoint)
            # logger.info(f"Test: GET Response Status: {response.status_code}")
            # logger.info(f"Test: GET Response Body: {response.json()}")

        except Exception as e:
            logger.error(f"Test failed: {e}", exc_info=True) 