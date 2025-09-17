from src.pkg.core.exception import CoreException


class ValidationException(CoreException):
    status_code = 422
    detail = "validation_exception"


class UnsupportedBytesSchemaException(CoreException):
    status_code = 400
    detail = "Unsupported bytes schema"


class SchemaRegistryException(CoreException):
    status_code = 400
    detail = "Schema registry exc"
