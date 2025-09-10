from src.pkg.core.exception import CoreException


class ValidationException(CoreException):
    status_code = 422
    detail = "validation_exception"
