"""
Tests for ``search_currency_schemes`` and the currency search methods'
shared limit/offset/sort/sortOrder pagination parameters.

Pinned here: the ``where`` param is only sent when a filter is given, a
failed request is re-raised as ``IRPAPIError``, and each of ``search_currencies``,
``search_currency_schemes``, and ``search_currency_scheme_vintages`` wires
limit/offset/sort/sortOrder into the request params the same way, rejecting
invalid values before any request is sent.
"""

import pytest

from irp_integration.constants import SEARCH_CURRENCY_SCHEMES
from irp_integration.exceptions import IRPAPIError, IRPValidationError

CURRENCY_SEARCH_METHODS = [
    "search_currencies",
    "search_currency_schemes",
    "search_currency_scheme_vintages",
]


def test_where_clause_is_sent_when_given(make_reference_data_manager, response):
    manager, client = make_reference_data_manager([response(200, json_body={'items': []})])

    manager.search_currency_schemes('currencySchemeCode="RMS"')

    assert client.calls[0]['path'] == SEARCH_CURRENCY_SCHEMES
    assert client.calls[0]['params'] == {'where': 'currencySchemeCode="RMS"'}


def test_where_clause_is_omitted_when_not_given(make_reference_data_manager, response):
    manager, client = make_reference_data_manager([response(200, json_body={'items': []})])

    manager.search_currency_schemes()

    assert client.calls[0]['params'] == {}


def test_request_failure_raises_irp_api_error(make_reference_data_manager):
    manager, client = make_reference_data_manager([RuntimeError("boom")])

    with pytest.raises(IRPAPIError):
        manager.search_currency_schemes()


@pytest.mark.parametrize("method_name", CURRENCY_SEARCH_METHODS)
def test_pagination_and_sort_params_are_sent_when_given(method_name, make_reference_data_manager, response):
    manager, client = make_reference_data_manager([response(200, json_body={'items': []})])

    getattr(manager, method_name)(
        where_clause='code="USD"',
        limit=50,
        offset=100,
        sort="code",
        sort_order=-1
    )

    assert client.calls[0]['params'] == {
        'where': 'code="USD"',
        'limit': 50,
        'offset': 100,
        'sort': 'code',
        'sortOrder': -1,
    }


@pytest.mark.parametrize("method_name", CURRENCY_SEARCH_METHODS)
@pytest.mark.parametrize("kwargs", [{'limit': 0}, {'offset': -1}, {'sort_order': 0}])
def test_invalid_pagination_params_raise_before_request(method_name, kwargs, make_reference_data_manager):
    manager, client = make_reference_data_manager([])

    with pytest.raises(IRPValidationError):
        getattr(manager, method_name)(**kwargs)

    assert client.calls == []
