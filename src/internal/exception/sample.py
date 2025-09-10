from src.pkg.core.exception import CoreException

__all__ = ["SampleCreateException", "SampleUpdateException"]


class SampleCreateException(CoreException):
    status_code = 400
    detail = "can`t create Sample"


class SampleUpdateException(CoreException):
    status_code = 400
    detail = "can`t update Sample"
