class MaxRetryAttemptsReachedException(Exception):
    """ A custom exception class for exceeding maximum number of retry attempts"""
    
    def __init__(self, message="Maximum retries reached"):
        self.message = message
        super().__init__(f"{message}")
class AccessDeniedException(Exception):
    """A custom exception class in case of IP restrictions """
    
    def __init__(self, message="Access denied"):
        self.message = message
        super().__init__(f"{message}")