"""Mock BeautifulSoup module for testing without parsing dependency"""

class MockTag:
    def __init__(self, name, text="", attrs=None):
        self.name = name
        self.string = text
        self.text = text
        self.attrs = attrs or {}
        
    def find(self, name, attrs=None):
        return MockTag(name, "Mock found content")
        
    def find_all(self, name, attrs=None):
        return [MockTag(name, f"Mock content {i}") for i in range(3)]
        
    def get(self, attr, default=None):
        return self.attrs.get(attr, default)
        
    def get_text(self, strip=False):
        return self.text.strip() if strip else self.text

class BeautifulSoup:
    def __init__(self, markup, features=None):
        self.markup = markup
        
    def find(self, name, attrs=None):
        return MockTag(name, "Mock soup content")
        
    def find_all(self, name, attrs=None):
        return [MockTag(name, f"Mock soup content {i}") for i in range(2)]
        
    def select(self, selector):
        return [MockTag("div", f"Mock selected content {i}") for i in range(2)]
        
    def select_one(self, selector):
        return MockTag("div", "Mock selected content")
