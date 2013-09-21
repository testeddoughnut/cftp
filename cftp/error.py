
class CftpError(Exception):
    pass

class UsageError(CftpError):
    """Error with parsing"""

class CatchExit(CftpError):
    """Overload for exiting"""