from src.pkg.logging import LoggingInit


def test_logging_init_format_contains_expected_fields():
    fmt = LoggingInit(lvl="INFO").format()
    # Ensure the format contains key placeholders used by our logs
    assert "{time:" in fmt
    assert "{level}" in fmt
    assert "{name}" in fmt and "{function}" in fmt and "{line}" in fmt
    assert "{extra[request_id]}" in fmt
