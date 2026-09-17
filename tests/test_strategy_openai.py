from parquet.config import StrategyConfig
from parquet.strategy_openai import _response_output_text


def test_strategy_config_accepts_openai_api() -> None:
    config = StrategyConfig(provider="openai_api")
    assert config.provider == "openai_api"


def test_response_output_text_prefers_direct_field() -> None:
    body = {"output_text": '{"schema_version":1}'}
    assert _response_output_text(body) == '{"schema_version":1}'


def test_response_output_text_extracts_message_content() -> None:
    body = {
        "output": [
            {"type": "web_search_call", "status": "completed"},
            {
                "type": "message",
                "content": [
                    {"type": "output_text", "text": '{"schema_version":1}'},
                ],
            },
        ]
    }
    assert _response_output_text(body) == '{"schema_version":1}'
