class PageNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)

class RateLimitException(Exception):
    def __init__(self, message):
        super().__init__(message)