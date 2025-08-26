"""Mock requests module for testing without network dependency"""

class MockResponse:
    def __init__(self, text="", status_code=200, headers=None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self.content = text.encode('utf-8')
        
    def json(self):
        import json
        return json.loads(self.text)
        
    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")

class HTTPError(Exception):
    pass

class RequestException(Exception):
    pass

class Timeout(RequestException):
    pass

class ConnectionError(RequestException):
    pass

def get(url, **kwargs):
    # Return mock response with basic HTML
    return MockResponse("""
    <html>
        <body>
            <div class="mock-content">Mock content</div>
        </body>
    </html>
    """)

def post(url, **kwargs):
    return MockResponse('{"status": "ok"}')

# Mock session
class Session:
    def get(self, url, **kwargs):
        return get(url, **kwargs)
        
    def post(self, url, **kwargs):
        return post(url, **kwargs)
        
    def close(self):
        pass

# Mock exceptions module
class exceptions:
    RequestException = RequestException
    Timeout = Timeout
    ConnectionError = ConnectionError
    HTTPError = HTTPError
