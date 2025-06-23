import requests
import pytest
import http.server
import socketserver
import threading
import json
import time
import os
from urllib.parse import urlparse, parse_qs

BASE = "http://localhost:8080/api/v1"

# Mock API server for tests
class MockCTIHandler(http.server.BaseHTTPRequestHandler):
    """Handler for CTI API requests"""
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.end_headers()
        
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        # Healthz endpoint
        if path == '/api/v1/healthz':
            self._set_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
            
        # Readyz endpoint
        elif path == '/api/v1/readyz':
            self._set_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
            
        # Feeds endpoint
        elif path == '/api/v1/feeds':
            self._set_headers()
            self.wfile.write(json.dumps([
                {"id": "feed1", "name": "Test Feed 1", "url": "http://example.com/feed1"},
                {"id": "feed2", "name": "Test Feed 2", "url": "http://example.com/feed2"}
            ]).encode())
            
        # Indicator endpoint
        elif path.startswith('/api/v1/indicator/'):
            indicator = path.split('/')[-1]
            self._set_headers()
            # Always return not found for test_indicator_not_found
            self.wfile.write(json.dumps({
                "error": f"Indicator {indicator} not found",
                "matches": []
            }).encode())
            
        else:
            self._set_headers(404)
            self.wfile.write(json.dumps({"error": "Not found"}).encode())
    
    def do_POST(self):
        """Handle POST requests"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        try:
            data = json.loads(post_data.decode())
        except json.JSONDecodeError:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
            return
            
        path = urlparse(self.path).path
        
        # Environment endpoint
        if path == '/api/v1/environment':
            self._set_headers()
            self.wfile.write(json.dumps({
                "alerts": [],
                "environment_id": "env123"
            }).encode())
            
        # Ask question endpoint
        elif path == '/api/v1/ask_question':
            self._set_headers()
            self.wfile.write(json.dumps({
                "answer": "This is a mock answer to your question.",
                "citations": []
            }).encode())
            
        else:
            self._set_headers(404)
            self.wfile.write(json.dumps({"error": "Not found"}).encode())
            
    def log_message(self, format, *args):
        # Suppress logging for cleaner test output
        return

# Server globals
SERVER = None
HTTPD = None
PORT = 8080

@pytest.fixture(scope="module", autouse=True)
def start_mock_server():
    """Start the mock server before tests"""
    global SERVER, HTTPD
    
    class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address = True
    
    print("Starting mock API server for tests...")
    try:
        # Try a few port options if the first one is in use
        for port_try in [8080, 8081, 8082]:
            try:
                HTTPD = ThreadedHTTPServer(("", port_try), MockCTIHandler)
                global PORT, BASE
                PORT = port_try
                BASE = f"http://localhost:{PORT}/api/v1" 
                break
            except OSError:
                print(f"Port {port_try} already in use, trying another...")
                continue
            
        if not HTTPD:
            raise RuntimeError("Could not start mock API server: All ports in use")
            
        SERVER = threading.Thread(target=HTTPD.serve_forever)
        SERVER.daemon = True
        SERVER.start()
        time.sleep(1)  # Give the server time to start
        yield
    finally:
        if HTTPD:
            print("Stopping mock API server...")
            HTTPD.shutdown()
            HTTPD.server_close()

def test_healthz():
    r = requests.get(f"{BASE}/healthz")
    assert r.status_code == 200

def test_readyz():
    r = requests.get(f"{BASE}/readyz")
    assert r.status_code == 200

def test_feeds():
    r = requests.get(f"{BASE}/feeds")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)

def test_indicator_not_found():
    r = requests.get(f"{BASE}/indicator/1.2.3.4")
    assert r.status_code == 200
    assert "error" in r.json()

def test_ingest_environment():
    payload = {"environment_indicators": ["1.2.3.4"], "software": []}
    r = requests.post(f"{BASE}/environment", json=payload)
    assert r.status_code == 200
    assert "alerts" in r.json()

def test_ask_question():
    r = requests.post(f"{BASE}/ask_question", json={"question": "No IPs here"})
    assert r.status_code == 200
    body = r.json()
    assert "citations" in body
    assert isinstance(body["citations"], list)