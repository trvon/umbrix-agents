from google.adk.tools import BaseTool
import requests
from .dead_link_integration import dead_link_aware_retry, get_dead_link_detector


class GeoIpLookupTool(BaseTool):
    """
    Tool to perform GeoIP lookup for a given IP address using a public HTTP API.
    Input: IP address string.
    Output: Dict containing country, region, city, latitude, and longitude.
    """
    def __init__(self):
        super().__init__(
            name="geoip_lookup",
            description="Lookup geographic information for an IP address via ip-api.com with dead link detection"
        )
        # Simple in-memory cache: ip -> geo data
        self.cache = {}
        self.dead_link_detector = get_dead_link_detector()

    @dead_link_aware_retry('geoip_services', 'processing', 'geoip_lookup')
    def call(self, ip_address: str):
        # Return cached result if available
        if ip_address in self.cache:
            return self.cache[ip_address]
        url = f"http://ip-api.com/json/{ip_address}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "success":
            raise Exception(f"GeoIP lookup failed: {data.get('message')}")
        result = {
            "ip": ip_address,
            "country": data.get("country"),
            "region": data.get("regionName"),
            "city": data.get("city"),
            "lat": data.get("lat"),
            "lon": data.get("lon")
        }
        # Cache and return
        self.cache[ip_address] = result
        return result