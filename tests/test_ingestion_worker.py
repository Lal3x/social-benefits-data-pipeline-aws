import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


def load_worker(monkeypatch):
    monkeypatch.setenv("BUCKET", "test-bucket")
    monkeypatch.setenv("SECRET_NAME", "test-secret")
    spec = importlib.util.spec_from_file_location(
        "ingestion_worker", Path("src/ingestion_worker/lambda_function.py")
    )
    module = importlib.util.module_from_spec(spec)
    with patch("boto3.client", return_value=MagicMock()):
        spec.loader.exec_module(module)
    return module


def aws_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "HeadObject")


def test_existing_object(monkeypatch):
    worker = load_worker(monkeypatch)
    worker.s3.head_object.return_value = {}
    assert worker.ja_existe("raw/file.json") is True


def test_missing_object(monkeypatch):
    worker = load_worker(monkeypatch)
    worker.s3.exceptions.ClientError = ClientError
    worker.s3.head_object.side_effect = aws_error("404")
    assert worker.ja_existe("raw/file.json") is False


def test_access_denied_is_not_hidden(monkeypatch):
    worker = load_worker(monkeypatch)
    worker.s3.exceptions.ClientError = ClientError
    worker.s3.head_object.side_effect = aws_error("AccessDenied")
    with pytest.raises(ClientError):
        worker.ja_existe("raw/file.json")
