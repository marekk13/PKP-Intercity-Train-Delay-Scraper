import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest

from get_delays import (
    NetworkTracker,
    extract_train_numbers,
    get_delays,
    process_single_train,
)


def test_extract_train_numbers_single():
    assert extract_train_numbers("1116") == [1116]
    assert extract_train_numbers("IC 1116") == [1116]
    assert extract_train_numbers("TLK 53100") == [53100]
    assert extract_train_numbers("EIC 1300") == [1300]
    assert extract_train_numbers("EIP 1600") == [1600]


def test_extract_train_numbers_with_slash():
    assert extract_train_numbers("1116/7") == [1116, 1117]
    assert extract_train_numbers("IC 1116/7") == [1116, 1117]
    assert extract_train_numbers("1116/1117") == [1116, 1117]
    assert extract_train_numbers("53100/1") == [53100, 53101]


def test_extract_train_numbers_empty():
    assert extract_train_numbers("") == []
    assert extract_train_numbers(None) == []
    assert extract_train_numbers("Brak") == []


def test_extract_train_numbers_int():
    assert extract_train_numbers(1116) == [1116]


def _make_response(url, status=200, method="GET", headers=None, text="[]"):
    return SimpleNamespace(
        url=url,
        status=status,
        request=SimpleNamespace(method=method),
        headers=headers or {},
        text=lambda: text,
    )


def test_network_tracker_detects_autocomplete_428_and_formats_summary():
    tracker = NetworkTracker()
    tracker.on_response(_make_response("https://example.com/pixel.js", status=404))
    assert tracker.format_summary() == "brak zarejestrowanych zapytań"

    tracker.on_response(
        _make_response(
            "https://portalpasazera.pl/Wyszukiwarka/WyszukajNumerPociagu?wprowadzonyTekst=1002",
            status=428,
            headers={"x-require-challenge": "1"},
        )
    )
    assert tracker.has_autocomplete_failure() is True
    summary = tracker.format_summary()
    assert "GET /Wyszukiwarka/WyszukajNumerPociagu?wprowadzonyTekst=1002 -> 428" in summary
    assert "challenge=1" in summary


def test_network_tracker_handles_200_empty_and_request_failed():
    tracker = NetworkTracker()
    tracker.on_response(
        _make_response(
            "https://portalpasazera.pl/Wyszukiwarka/WyszukajNumerPociagu?wprowadzonyTekst=647",
            status=200,
            text="[]",
        )
    )
    assert tracker.has_autocomplete_failure() is False
    assert "empty=[]" in tracker.format_summary()

    tracker.clear()
    assert tracker.has_autocomplete_failure() is True

    tracker.on_request_failed(
        SimpleNamespace(
            url="https://portalpasazera.pl/Wyszukiwarka/WyszukajNumerPociagu?wprowadzonyTekst=1002",
            method="GET",
            failure="net::ERR_CONNECTION_RESET",
        )
    )
    assert tracker.has_autocomplete_failure() is True
    assert "FAILED" in tracker.format_summary()
    assert "ERR_CONNECTION_RESET" in tracker.format_summary()


def test_process_single_train_retries_autocomplete_error_but_not_invalid_train_number():
    page = MagicMock()
    logger = logging.getLogger("test_retry")

    tracker = NetworkTracker()
    train_retry = {"number": "1002"}
    with patch("get_delays.time.sleep"), patch(
        "get_delays.get_train_details", side_effect=["autocomplete_error", [{"station": "A"}]]
    ) as mock_details:
        process_single_train(page, train_retry, logger, network_tracker=tracker)

    assert mock_details.call_count == 2
    assert train_retry["delay_info"] == [{"station": "A"}]

    train_invalid = {"number": "647"}
    with patch("get_delays.time.sleep"), patch(
        "get_delays.get_train_details", return_value="invalid_train_number"
    ) as mock_details_invalid:
        process_single_train(page, train_invalid, logger, network_tracker=tracker)

    assert mock_details_invalid.call_count == 1
    assert train_invalid["delay_info"] == "invalid_train_number"


def _mock_playwright_cm():
    browser = MagicMock()
    context = MagicMock()
    page = MagicMock()
    browser.new_context.return_value = context
    context.new_page.return_value = page
    p = MagicMock()
    p.chromium.launch.return_value = browser
    cm = MagicMock()
    cm.__enter__.return_value = p
    cm.__exit__.return_value = False
    return cm, browser


def test_get_delays_circuit_breaker_aborts_on_consecutive_invalid_numbers():
    cm, browser = _mock_playwright_cm()
    trains = [{"number": str(1000 + i)} for i in range(6)]
    processed = []

    def fake_process(page, train, logger, target_date=None, network_tracker=None):
        processed.append(train["number"])
        train["delay_info"] = "invalid_train_number"

    with patch("get_delays.sync_playwright", return_value=cm), patch(
        "get_delays.process_single_train", side_effect=fake_process
    ):
        with pytest.raises(RuntimeError, match="3 kolejnych błędów"):
            get_delays(trains, logger=logging.getLogger("test_cb"), max_consecutive_invalid_numbers=3)

    assert processed == ["1000", "1001", "1002"]
    browser.close.assert_called_once()


def test_get_delays_circuit_breaker_resets_on_success_and_na_but_not_timeout():
    cm, browser = _mock_playwright_cm()
    trains = [{"number": str(i)} for i in range(5)]
    outcomes = iter(["invalid_train_number", "page_load_timeout", "N/A", "invalid_train_number", [{"station": "X"}]])

    def fake_process(page, train, logger, target_date=None, network_tracker=None):
        train["delay_info"] = next(outcomes)

    with patch("get_delays.sync_playwright", return_value=cm), patch(
        "get_delays.process_single_train", side_effect=fake_process
    ):
        res = get_delays(trains, logger=logging.getLogger("test_cb_reset"), max_consecutive_invalid_numbers=2)

    assert [r["delay_info"] for r in res] == [
        "invalid_train_number",
        "page_load_timeout",
        "N/A",
        "invalid_train_number",
        [{"station": "X"}],
    ]
    browser.close.assert_called_once()
