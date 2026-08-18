import math
from decimal import Decimal

import requests

from ops.metrics import analytics_gauges


def _values(**overrides):
    values = {
        name: index + 1
        for index, name in enumerate(analytics_gauges._DATA_GAUGES)
    }
    values.update(overrides)
    return values


def _response(mocker, payload):
    response = mocker.MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_ops_and_dbt_runner_metric_names_are_the_same_contract():
    from dbt_runner import app as dbt_runner_app

    assert set(analytics_gauges._DATA_GAUGES) == (
        dbt_runner_app._ANALYTICS_METRIC_NAMES
    )


def test_complete_refresh_updates_all_gauges_and_freshness(mocker):
    values = _values(cartracker_stale_listings_pct=Decimal("4.25"))
    get = mocker.patch(
        "ops.metrics.analytics_gauges.http_requests.get",
        return_value=_response(
            mocker,
            {"ok": True, "values": values, "errors": {}},
        ),
    )
    mocker.patch("ops.metrics.analytics_gauges.time.time", return_value=1234.5)

    analytics_gauges.update_analytics_metrics()

    for name, gauge in analytics_gauges._DATA_GAUGES.items():
        assert gauge._value.get() == float(values[name])
    assert (
        analytics_gauges.cartracker_metrics_last_success_timestamp_seconds._value.get()
        == 1234.5
    )
    get.assert_called_once_with(
        f"{analytics_gauges._ANALYTICS_READER_URL}/analytics/metrics",
        timeout=analytics_gauges._REQUEST_TIMEOUT_SECONDS,
    )


def test_partial_refresh_sets_only_failed_gauge_nan_and_keeps_freshness(mocker):
    failed_name = "cartracker_block_events_last_hour"
    values = _values(**{failed_name: None})
    mocker.patch(
        "ops.metrics.analytics_gauges.http_requests.get",
        return_value=_response(
            mocker,
            {"ok": False, "values": values, "errors": {failed_name: "missing"}},
        ),
    )
    freshness = analytics_gauges.cartracker_metrics_last_success_timestamp_seconds
    freshness.set(111.0)

    analytics_gauges.update_analytics_metrics()

    assert math.isnan(analytics_gauges._DATA_GAUGES[failed_name]._value.get())
    assert analytics_gauges.cartracker_observation_count_last_hour._value.get() == values[
        "cartracker_observation_count_last_hour"
    ]
    assert freshness._value.get() == 111.0


def test_transport_failure_sets_all_data_gauges_nan(mocker):
    mocker.patch(
        "ops.metrics.analytics_gauges.http_requests.get",
        side_effect=requests.ConnectionError("dbt runner unavailable"),
    )
    freshness = analytics_gauges.cartracker_metrics_last_success_timestamp_seconds
    freshness.set(222.0)

    analytics_gauges.update_analytics_metrics()

    assert all(
        math.isnan(gauge._value.get())
        for gauge in analytics_gauges._DATA_GAUGES.values()
    )
    assert freshness._value.get() == 222.0


def test_invalid_payload_is_a_failed_refresh(mocker):
    mocker.patch(
        "ops.metrics.analytics_gauges.http_requests.get",
        return_value=_response(mocker, {"ok": True, "values": []}),
    )
    freshness = analytics_gauges.cartracker_metrics_last_success_timestamp_seconds
    freshness.set(333.0)

    analytics_gauges.update_analytics_metrics()

    assert all(
        math.isnan(gauge._value.get())
        for gauge in analytics_gauges._DATA_GAUGES.values()
    )
    assert freshness._value.get() == 333.0


def test_non_object_payload_is_a_failed_refresh(mocker):
    mocker.patch(
        "ops.metrics.analytics_gauges.http_requests.get",
        return_value=_response(mocker, []),
    )
    freshness = analytics_gauges.cartracker_metrics_last_success_timestamp_seconds
    freshness.set(444.0)

    analytics_gauges.update_analytics_metrics()

    assert all(
        math.isnan(gauge._value.get())
        for gauge in analytics_gauges._DATA_GAUGES.values()
    )
    assert freshness._value.get() == 444.0
