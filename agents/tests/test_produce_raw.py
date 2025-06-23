import json
import pytest
from unittest.mock import patch

import tests.produce_raw as produce_raw


def test_produce_message_success(monkeypatch):
    # Mock KafkaProducer to avoid real Kafka interaction
    class DummyProducer:
        def __init__(self, *args, **kwargs):
            pass
        def send(self, topic, message):
            assert topic == 'raw.intel'
            # message should be serializable
            assert isinstance(message, dict)
        def flush(self):
            pass

    monkeypatch.setattr(produce_raw, 'KafkaProducer', DummyProducer)
    result = produce_raw.produce_message()
    assert result is True 