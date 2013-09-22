
class CftpError(Exception):
    pass

class UsageError(CftpError):
    """Error with parsing"""

class CatchExit(CftpError):
    """Overload for exiting"""

class NoSuchContainer(CftpError):
    """Requested container doesn't exist"""

class NoSuchObject(CftpError):
    """Requested object doesn't exist""" 

class ObjectIsSubDir(CftpError):
    """Requested object is a subdir"""