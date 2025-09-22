import re

from src.pkg.context import get_tx_id, make_tx_id


def test_tx_id_generation_and_replacement():
    # First generation
    make_tx_id()
    tx1 = get_tx_id()
    assert isinstance(tx1, str) and len(tx1) > 0
    assert re.fullmatch(r"[0-9a-f]{32}", tx1) is not None

    # New generation produces a different id
    make_tx_id()
    tx2 = get_tx_id()
    assert tx2 != tx1
    assert re.fullmatch(r"[0-9a-f]{32}", tx2) is not None
