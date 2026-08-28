import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, Path(path))
    module = importlib.util.module_from_spec(spec)
    with patch("boto3.client", return_value=MagicMock()):
        spec.loader.exec_module(module)
    return module


@pytest.fixture
def detector(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:region:account:stateMachine:test")
    monkeypatch.setenv("SECRET_NAME", "test-secret")
    return load_module("src/detector/lambda_function.py", "detector")


def test_next_month_rolls_year(detector):
    assert detector._proximo_mes(2026, 12) == (2027, 1)
    assert detector._proximo_mes(2026, 8) == (2026, 9)


def test_last_processed_month(detector):
    assert detector._ultimo_mes_processado({(2026, 7), (2026, 8)}) == (2026, 8)
    assert detector._ultimo_mes_processado(set()) is None


@pytest.mark.parametrize("value", ["20261", "202613", "abc123"])
def test_forced_month_is_validated(detector, value):
    detector._listar_meses_com_sucesso = MagicMock(return_value={(2026, 7)})
    with pytest.raises(ValueError):
        detector.lambda_handler({"mes_forcado": value}, None)


def test_no_history_does_not_call_api(detector):
    detector._listar_meses_com_sucesso = MagicMock(return_value=set())
    assert detector.lambda_handler({}, None) == {
        "acao": "nenhuma",
        "motivo": "sem_historico",
    }


def test_processed_forced_month_is_idempotent(detector):
    detector._listar_meses_com_sucesso = MagicMock(return_value={(2026, 8)})
    assert detector.lambda_handler({"mes_forcado": "202608"}, None) == {
        "acao": "nenhuma",
        "motivo": "mes_ja_processado",
    }
