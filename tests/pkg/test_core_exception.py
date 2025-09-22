from src.pkg.core.exception import CoreException


class MyExc(CoreException):
    status_code = 418
    detail = "I'm a teapot"


def test_core_exception_subclass_and_openapi():
    exc = MyExc()
    assert exc.status_code == 418
    assert exc.detail == "I'm a teapot"

    doc = MyExc.generate_openapi()
    assert 418 in doc
    example = doc[418]["content"]["application/json"]["example"]["exception"]["message"]
    assert example == "I'm a teapot"
