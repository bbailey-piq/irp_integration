"""
Portfolio management operations.

Handles portfolio creation, retrieval, and geocoding/hazard operations.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, TYPE_CHECKING
from zoneinfo import ZoneInfo

from .constants import GET_PORTFOLIO_BY_ID, GET_PORTFOLIO_METADATA, CREATE_PORTFOLIO, GET_GEOHAZ_JOB, SEARCH_PORTFOLIOS, GEOHAZ_PORTFOLIO, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES, SEARCH_ACCOUNTS_BY_PORTFOLIO, SEARCH_ACCOUNTS, SEARCH_POLICIES, SEARCH_LOCATIONS, ADD_FILTERED_ACCOUNTS, MANAGE_ACCOUNTS_BY_PORTFOLIO
from .exceptions import IRPAPIError, IRPJobError, IRPValidationError
from .validators import validate_list_not_empty, validate_list_of_positive_ints, validate_non_empty_string, validate_non_negative_int, validate_positive_int
from .utils import extract_id_from_location_header

if TYPE_CHECKING:
    from . import IRPClient
    from .edm import EDMManager

logger = logging.getLogger(__name__)


class PortfolioManager:
    """Manager for portfolio operations."""

    def __init__(self, irp: "IRPClient") -> None:
        """
        Initialize portfolio manager.

        Args:
            irp: Owning IRP client instance
        """
        self._irp = irp
        self.client = irp.client

    @property
    def edm_manager(self) -> "EDMManager":
        """Return the owning client's EDM manager."""
        return self._irp.edm


    def get_portfolio_by_id(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]:
        """
        Retrieve portfolio details by portfolio ID.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID

        Returns:
            Dict containing portfolio details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        try:
            response = self.client.request('GET', GET_PORTFOLIO_BY_ID.format(exposureId=exposure_id, id=portfolio_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get portfolio details for exposure ID '{exposure_id}' and portfolio ID '{portfolio_id}': {e}")
        
    def get_portfolio_metadata(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]:
        """
        Retrieve portfolio metadata by portfolio ID.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID

        Returns:
            Dict containing portfolio metadata details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        try:
            response = self.client.request('GET', GET_PORTFOLIO_METADATA.format(exposureId=exposure_id, id=portfolio_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get portfolio metadata for exposure ID '{exposure_id}' and portfolio ID '{portfolio_id}': {e}")
    
    def search_portfolios(self, exposure_id: int, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search portfolios within an exposure.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string for portfolio names
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of portfolio dictionaries
        """
        validate_positive_int(exposure_id, "exposure_id")

        params: Dict[str, Any] = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request(
                'GET',
                SEARCH_PORTFOLIOS.format(exposureId=exposure_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search portfolios for exposure ID '{exposure_id}': {e}")

    def search_portfolios_paginated(self, exposure_id: int, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search all portfolios within an exposure with automatic pagination.

        Fetches all pages of results matching the filter criteria.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string for portfolio names

        Returns:
            Complete list of all matching portfolios across all pages
        """
        validate_positive_int(exposure_id, "exposure_id")

        all_results = []
        offset = 0
        limit = 100

        while True:
            results = self.search_portfolios(exposure_id=exposure_id, filter=filter, limit=limit, offset=offset)
            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results


    def search_accounts_by_portfolio(
        self,
        exposure_id: int,
        portfolio_id: int,
        filter: str = "",
        sort: str = "",
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve accounts within a portfolio.

        Filterable properties for this endpoint are a closed list: accountid,
        accountName, accountNumber, branchName, cedantName, ownerName,
        producerName, underwriterName. Line of business and state are *not*
        among them, and neither is filterable on ``search_accounts`` either.
        LOB lives on policies and admin1 (state/province) on locations, so
        those selections go through ``search_policies`` and
        ``search_locations``, joined back to accounts on accountId.

        ``limit`` and ``offset`` default to None because the Risk Data API does
        not declare them for this operation, unlike its siblings. Omitting them
        preserves the endpoint's own default paging behavior; supply them only
        when deliberately paging (see
        ``search_accounts_by_portfolio_paginated``).

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID
            filter: Optional filter expression over the properties listed above
            sort: Optional comma-delimited sort properties, each optionally
                suffixed with ASC or DESC
            limit: Maximum results per page; omitted from the request when None
                (default: None)
            offset: Offset for pagination; omitted from the request when None
                (default: None)

        Returns:
            List of account dicts

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")
        if limit is not None:
            validate_positive_int(limit, "limit")
        if offset is not None:
            validate_non_negative_int(offset, "offset")

        params: Dict[str, Any] = {}
        if filter:
            params['filter'] = filter
        if sort:
            params['sort'] = sort
        if limit is not None:
            params['limit'] = limit
        if offset is not None:
            params['offset'] = offset

        try:
            response = self.client.request(
                'GET',
                SEARCH_ACCOUNTS_BY_PORTFOLIO.format(exposureId=exposure_id, id=portfolio_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search portfolio accounts for exposure ID '{exposure_id}' and portfolio ID '{portfolio_id}': {e}")

    def search_accounts_by_portfolio_paginated(
        self,
        exposure_id: int,
        portfolio_id: int,
        filter: str = "",
        sort: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all accounts within a portfolio with automatic pagination.

        Fetches all pages of results matching the filter criteria. Because the
        API does not declare limit/offset for this operation, two guards bound
        the loop: a page larger than the requested limit means the server
        ignored pagination and already returned everything, and a page holding
        only already-seen account IDs means paging is not advancing. Both are
        logged as warnings.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID
            filter: Optional filter expression; see
                ``search_accounts_by_portfolio`` for supported properties
            sort: Optional comma-delimited sort properties

        Returns:
            Complete list of all matching accounts across all pages

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        all_results: List[Dict[str, Any]] = []
        seen_account_ids: set = set()
        offset = 0
        limit = 100

        while True:
            results = self.search_accounts_by_portfolio(
                exposure_id=exposure_id,
                portfolio_id=portfolio_id,
                filter=filter,
                sort=sort,
                limit=limit,
                offset=offset
            )

            # More rows than we asked for means the server ignored the paging
            # params, so this single response is already the complete set
            if len(results) > limit:
                logger.warning(
                    "Portfolio ID %s account search ignored pagination (%s rows for limit %s); returning the single response",
                    portfolio_id, len(results), limit
                )
                return results

            page_account_ids = {account.get('accountId') for account in results if account.get('accountId') is not None}
            if page_account_ids and page_account_ids <= seen_account_ids:
                logger.warning(
                    "Portfolio ID %s account search returned an already-seen page at offset %s; stopping. Results may be incomplete.",
                    portfolio_id, offset
                )
                break
            seen_account_ids |= page_account_ids

            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results

    def search_accounts(
        self,
        exposure_id: int,
        filter: str = "",
        sort: str = "",
        limit: int = 100,
        offset: int = 0,
        allow_deep_filters: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Search accounts across an entire EDM.

        Wider filter surface than the portfolio-scoped
        ``search_accounts_by_portfolio``: adds locationsCount, tagIds,
        policyExpirationDate, stampDate, reportsCount, resultsCount, userId1-4
        and userText1-2 to the account properties.

        Filter grammar: ``property <operator> value``, where operator is one of
        =, !=, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, IS NULL, IS NOT NULL,
        combined with AND/OR. String literals are double-quoted, IN lists are
        parenthesised, and ``*`` is the wildcard. List operators are not
        supported on YYYY-MM-DD properties; use range comparisons instead.

        Line of business and state are not filterable here either. Use
        ``search_policies`` (LOB) and ``search_locations`` (admin1Code /
        admin1Name) for those selections and join back on accountId — both are
        documented and neither needs ``allow_deep_filters``.

        ``allow_deep_filters`` is the only parameter of its kind in the API and
        is effectively undocumented: the reference offers the single sentence
        "If true, this search was triggered from portfolio", with no default
        and no definition of "deep", and the filtering guide's one example
        passes it as false. It might widen the filterable property set beyond
        the account itself, but that is a guess, and the documented route above
        makes it unnecessary. Left exposed for probing, defaulting to False.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter expression over the account properties
            sort: Optional comma-delimited sort properties, each optionally
                suffixed with ASC or DESC
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)
            allow_deep_filters: Allow filtering on properties outside the
                account itself (default: False)

        Returns:
            List of account dicts

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(limit, "limit")
        validate_non_negative_int(offset, "offset")

        # requests renders a Python bool as "True"/"False"; the API expects lowercase
        params: Dict[str, Any] = {
            'limit': limit,
            'offset': offset,
            'allowDeepFilters': 'true' if allow_deep_filters else 'false'
        }
        if filter:
            params['filter'] = filter
        if sort:
            params['sort'] = sort

        try:
            response = self.client.request(
                'GET',
                SEARCH_ACCOUNTS.format(exposureId=exposure_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search accounts for exposure ID '{exposure_id}': {e}")

    def search_accounts_paginated(
        self,
        exposure_id: int,
        filter: str = "",
        sort: str = "",
        allow_deep_filters: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Search all accounts within an EDM with automatic pagination.

        Fetches all pages of results matching the filter criteria. A page
        holding only already-seen account IDs stops the loop with a warning:
        the API describes offset as a page number while its filtering guide
        describes it as a record offset, so a page that fails to advance is
        treated as broken paging rather than looped over.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter expression; see ``search_accounts`` for the
                supported properties and grammar
            sort: Optional comma-delimited sort properties
            allow_deep_filters: Allow filtering on properties outside the
                account itself (default: False)

        Returns:
            Complete list of all matching accounts across all pages

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")

        all_results: List[Dict[str, Any]] = []
        seen_account_ids: set = set()
        offset = 0
        limit = 100

        while True:
            results = self.search_accounts(
                exposure_id=exposure_id,
                filter=filter,
                sort=sort,
                limit=limit,
                offset=offset,
                allow_deep_filters=allow_deep_filters
            )

            page_account_ids = {account.get('accountId') for account in results if account.get('accountId') is not None}
            if page_account_ids and page_account_ids <= seen_account_ids:
                logger.warning(
                    "Account search for exposure ID %s returned an already-seen page at offset %s; stopping. Results may be incomplete.",
                    exposure_id, offset
                )
                break
            seen_account_ids |= page_account_ids

            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results

    def search_policies(
        self,
        exposure_id: int,
        filter: str = "",
        sort: str = "",
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Search policies across an entire EDM.

        This is how a sub-portfolio breakout selects accounts by line of
        business. LOB is not a filterable property on any operation in the API,
        but every policy carries both its account and its LOB, so scope the
        search to a portfolio's accounts and group the results::

            accounts = pm.search_accounts_by_portfolio_paginated(edm_id, portfolio_id)
            ids = ','.join(str(account['accountId']) for account in accounts)
            policies = pm.search_policies_paginated(edm_id, filter=f'accountId IN ({ids})')

            by_lob: Dict[str, set] = {}
            for policy in policies:
                by_lob.setdefault(policy['lob']['lobName'], set()).add(policy['accountId'])

        A single pass yields every LOB bucket at once, and an account writing
        several lines of business lands in each of them — correct behavior,
        since portfolios hold whole accounts.

        Note that ``filter`` travels in the URL, so a long ``accountId IN (...)``
        list can exceed the server's URL length limit. Chunk the account IDs
        across several calls, or omit the filter and intersect against the
        portfolio's accounts client-side.

        Filterable properties (closed list): accountId, aggregateLimit,
        aggregateMaxDeductible, aggregateMinDeductible, attachmentPoint,
        blanketDeductible, blanketLimit, blanketPremium, coverageBase,
        currency, expirationDate, inceptionDate, isFranchiseDeductible,
        limitGU, maxDeductible, minDeductible, newCauseOfLoss, partOf,
        percentOfLossDeductible, peril, policyId, policyNumber, status,
        structure, userText1, userText2, userText3, userText4. See
        ``search_accounts`` for the filter grammar.

        ``lobId`` is listed as *sortable* for this operation but is absent from
        the filterable list. Whether that omission is real or a documentation
        gap is worth a single probe against a live tenant: a working
        ``lobId = <id>`` predicate would reduce the grouping above to one
        server-side call per LOB.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter expression over the properties listed above
            sort: Optional comma-delimited sort properties, each optionally
                suffixed with ASC or DESC
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of policy dicts, each carrying accountId and a nested
            lob dict of {lobId, lobName, uri}

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(limit, "limit")
        validate_non_negative_int(offset, "offset")

        params: Dict[str, Any] = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter
        if sort:
            params['sort'] = sort

        try:
            response = self.client.request(
                'GET',
                SEARCH_POLICIES.format(exposureId=exposure_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search policies for exposure ID '{exposure_id}': {e}")

    def search_policies_paginated(
        self,
        exposure_id: int,
        filter: str = "",
        sort: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Search all policies within an EDM with automatic pagination.

        Fetches all pages of results matching the filter criteria. A page
        holding only already-seen policy IDs stops the loop with a warning, on
        the same reasoning as ``search_accounts_paginated``.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter expression; see ``search_policies`` for the
                supported properties
            sort: Optional comma-delimited sort properties

        Returns:
            Complete list of all matching policies across all pages

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")

        all_results: List[Dict[str, Any]] = []
        seen_policy_ids: set = set()
        offset = 0
        limit = 100

        while True:
            results = self.search_policies(
                exposure_id=exposure_id,
                filter=filter,
                sort=sort,
                limit=limit,
                offset=offset
            )

            page_policy_ids = {policy.get('policyId') for policy in results if policy.get('policyId') is not None}
            if page_policy_ids and page_policy_ids <= seen_policy_ids:
                logger.warning(
                    "Policy search for exposure ID %s returned an already-seen page at offset %s; stopping. Results may be incomplete.",
                    exposure_id, offset
                )
                break
            seen_policy_ids |= page_policy_ids

            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results

    def search_locations(
        self,
        exposure_id: int,
        filter: str = "",
        sort: str = "",
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Search locations across an entire EDM.

        This is how a sub-portfolio breakout selects accounts by state or
        province, and unlike the LOB case it resolves entirely server-side:
        admin1Code and admin1Name are filterable alongside accountId, so one
        query returns exactly the locations in a state and their accounts are
        the accounts to add::

            filter = f'accountId IN ({ids}) AND admin1Code = "FL"'
            locations = pm.search_locations_paginated(edm_id, filter=filter)
            account_ids = {
                location['location']['property']['accountId'] for location in locations
            }

        admin1 is the first-level administrative division, so non-US provinces
        and regions use the same attribute. Whether a given EDM populates codes
        ("FL"), names ("Florida"), or both is data-dependent and worth
        confirming against the tenant before relying on either.

        Results are nested: each item is {location, propertyReference}, where
        ``location.property`` carries accountId and locationId, and
        ``location.address`` carries admin1Code and admin1Name.

        The URL-length caveat on ``search_policies`` applies here too.

        Filterable properties (closed list): accountId, addressType,
        admin1Code, admin1Name, admin2Code, admin2Name, admin3Code,
        admin3Name, admin4Code, admin4Name, area, areaUnit, bldgHeight,
        bldgValuation, block, blockGroup, buildingClass, buildingClassScheme,
        buildingId, buildingName, buildings, cityCode, cityName,
        contentLossTrigger, country, countryRmsCode, countryScheme, currency,
        dwellTime, expireDate, floodDefHtAboveGrnd, floodDefenseElevation,
        floodDefenseElevationUnit, floorArea, floorOccupancy,
        geoResolutionCode, heightUnit, huZone, inceptDate, isPrimaryBldg,
        latitude, locationCode, locationId, locationName, locationNumber,
        longitude, mfdSubcategory, nship, occupancyType, occupancyTypeScheme,
        otherZone, postalCode, propertyReference, rentalPropertyIdentifier,
        siteName, slope, stories, streetAddress, tiv, updateDate,
        useContentValue, userBfe, userGroundElev, userId1, userId2, userText1,
        userText2, yearBuilt, zone1, zone3Name, zone4Name. See
        ``search_accounts`` for the filter grammar.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter expression over the properties listed above
            sort: Optional comma-delimited sort properties, each optionally
                suffixed with ASC or DESC
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of location search item dicts, each wrapping a nested
            location dict

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(limit, "limit")
        validate_non_negative_int(offset, "offset")

        params: Dict[str, Any] = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter
        if sort:
            params['sort'] = sort

        try:
            response = self.client.request(
                'GET',
                SEARCH_LOCATIONS.format(exposureId=exposure_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search locations for exposure ID '{exposure_id}': {e}")

    def search_locations_paginated(
        self,
        exposure_id: int,
        filter: str = "",
        sort: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Search all locations within an EDM with automatic pagination.

        Fetches all pages of results matching the filter criteria. A page
        holding only already-seen location IDs stops the loop with a warning,
        on the same reasoning as ``search_accounts_paginated``.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter expression; see ``search_locations`` for
                the supported properties
            sort: Optional comma-delimited sort properties

        Returns:
            Complete list of all matching location search items across all
            pages

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")

        all_results: List[Dict[str, Any]] = []
        seen_location_ids: set = set()
        offset = 0
        limit = 100

        while True:
            results = self.search_locations(
                exposure_id=exposure_id,
                filter=filter,
                sort=sort,
                limit=limit,
                offset=offset
            )

            page_location_ids = set()
            for item in results:
                location_id = ((item.get('location') or {}).get('property') or {}).get('locationId')
                if location_id is not None:
                    page_location_ids.add(location_id)

            if page_location_ids and page_location_ids <= seen_location_ids:
                logger.warning(
                    "Location search for exposure ID %s returned an already-seen page at offset %s; stopping. Results may be incomplete.",
                    exposure_id, offset
                )
                break
            seen_location_ids |= page_location_ids

            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results

    def add_filtered_accounts(
        self,
        exposure_id: int,
        portfolio_id: int,
        *,
        marked_accounts: Optional[List[int]] = None,
        query_filter: str = "",
        select_all: bool = False,
        manage_existing_accounts: bool = False
    ) -> Dict[str, Any]:
        """
        Add accounts to a portfolio by ID list, by query filter, or both.

        Wraps the synchronous ``manageFilteredAccounts`` operation. HTTP 200
        ("Accounts added to portfolio") is its only documented success status,
        and it declares no response body, so an empty dict is returned on
        success. Any other 2xx raises rather than being normalised or polled —
        the legacy riskmodeler equivalents are asynchronous, but this Platform
        operation is not, and silently accepting a 202 here would hide that
        the request went down a different path.

        ``manage_portfolio_accounts`` is generally the better choice for adding
        an explicit list of account IDs: it reports how many accounts were
        actually added, which this operation cannot.

        Body semantics, as documented by the API: ``selectAll`` adds every
        account matched by ``queryFilter`` — every account in the EDM when no
        filter is given — and overrides ``markedAccounts``.
        ``manageExistingAccounts`` means "only existing accounts can be added
        to portfolio. All specified markedAccount values are ignored. All
        specified queryFilter values are ignored", which reads as a mode switch
        rather than an upsert flag; its real behavior is unverified. The
        grammar of ``queryFilter`` is not documented and is not stated to match
        the ``filter`` query parameter used by the search operations; it is
        transported verbatim.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID
            marked_accounts: Account IDs to add (default: None)
            query_filter: Expression selecting the accounts to add, passed
                through unmodified (default: "")
            select_all: Add every account matched by query_filter, or every
                account in the EDM when query_filter is empty. Overrides
                marked_accounts (default: False)
            manage_existing_accounts: Restrict the operation to accounts
                already in the portfolio (default: False)

        Returns:
            Parsed response body, or an empty dict when the response has none

        Raises:
            IRPValidationError: If parameters are invalid, or if no accounts
                were selected by any means
            IRPAPIError: If the request fails or returns an unexpected status
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        if marked_accounts is None:
            marked_accounts = []
        validate_list_of_positive_ints(marked_accounts, "marked_accounts")

        # An empty filter with selectAll would add every account in the EDM,
        # so that has to be asked for explicitly rather than fallen into
        if not marked_accounts and not query_filter and not select_all:
            raise IRPValidationError(
                "No accounts selected: provide marked_accounts or query_filter, "
                "or pass select_all=True to add every account in the EDM"
            )

        data: Dict[str, Any] = {
            "selectAll": select_all,
            "queryFilter": query_filter,
            "markedAccounts": marked_accounts,
            "manageExistingAccounts": manage_existing_accounts,
        }

        try:
            logger.info(
                "Adding filtered accounts to portfolio ID %s (marked accounts: %s, select all: %s)",
                portfolio_id, len(marked_accounts), select_all
            )
            response = self.client.request(
                'PUT',
                ADD_FILTERED_ACCOUNTS.format(exposureId=exposure_id, id=portfolio_id),
                json=data
            )
        except Exception as e:
            raise IRPAPIError(
                f"Failed to add filtered accounts to portfolio ID '{portfolio_id}' in exposure ID '{exposure_id}': {e}"
            ) from e

        if response.status_code != 200:
            raise IRPAPIError(
                f"Unexpected status {response.status_code} adding filtered accounts to portfolio ID "
                f"'{portfolio_id}' (expected 200); location header: {response.headers.get('location')}"
            )

        try:
            return response.json()
        except ValueError:
            # The documented 200 carries no body
            return {}

    def manage_portfolio_accounts(
        self,
        exposure_id: int,
        portfolio_id: int,
        *,
        accounts_to_add: Optional[List[int]] = None,
        accounts_to_remove: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Add and/or remove accounts on a portfolio by account ID.

        Wraps the synchronous ``managePortfolioAccounts`` operation. Unlike
        ``add_filtered_accounts`` it reports what it did, returning counts of
        the form::

            {"addAccounts": {"completed": 4, "total": 4},
             "removeAccounts": {"completed": 0, "total": 0}}

        which makes it the better choice for populating a sub-portfolio from a
        known list of account IDs. A partial result (completed < total) is
        logged but not treated as an error; inspect the returned counts if the
        caller needs to act on it.

        HTTP 200 is the only documented success status and any other 2xx
        raises. The API documents HTTP 403 for this operation as the caller
        lacking the "Edit Portfolios" action.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID
            accounts_to_add: Account IDs to add to the portfolio
                (default: None)
            accounts_to_remove: Account IDs to remove from the portfolio
                (default: None)

        Returns:
            Dict of add/remove counts as shown above, or an empty dict when the
            response has no body

        Raises:
            IRPValidationError: If parameters are invalid, or if both account
                lists are empty
            IRPAPIError: If the request fails or returns an unexpected status
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        if accounts_to_add is None:
            accounts_to_add = []
        if accounts_to_remove is None:
            accounts_to_remove = []
        validate_list_of_positive_ints(accounts_to_add, "accounts_to_add")
        validate_list_of_positive_ints(accounts_to_remove, "accounts_to_remove")

        if not accounts_to_add and not accounts_to_remove:
            raise IRPValidationError(
                "No accounts specified: provide at least one account ID in "
                "accounts_to_add or accounts_to_remove"
            )

        # Both fields are required by the API, so send both even when empty
        data: Dict[str, Any] = {
            "accountsToAdd": accounts_to_add,
            "accountsToRemove": accounts_to_remove,
        }

        try:
            logger.info(
                "Managing accounts on portfolio ID %s (adding: %s, removing: %s)",
                portfolio_id, len(accounts_to_add), len(accounts_to_remove)
            )
            response = self.client.request(
                'PATCH',
                MANAGE_ACCOUNTS_BY_PORTFOLIO.format(exposureId=exposure_id, id=portfolio_id),
                json=data
            )
        except Exception as e:
            raise IRPAPIError(
                f"Failed to manage accounts on portfolio ID '{portfolio_id}' in exposure ID '{exposure_id}': {e}"
            ) from e

        if response.status_code != 200:
            raise IRPAPIError(
                f"Unexpected status {response.status_code} managing accounts on portfolio ID "
                f"'{portfolio_id}' (expected 200); location header: {response.headers.get('location')}"
            )

        try:
            result = response.json()
        except ValueError:
            return {}

        logger.info("Portfolio ID %s accounts managed — %s", portfolio_id, result)
        return result


    def create_portfolios(self, portfolio_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Create multiple portfolios.

        Args:
            portfolio_data_list: List of portfolio data dicts, each containing:
                - edm_name: str
                - portfolio_name: str
                - portfolio_number: str
                - description: str

        Returns:
            List of portfolio IDs

        Raises:
            IRPValidationError: If portfolio_data_list is empty or invalid
            IRPAPIError: If portfolio creation fails or duplicate names exist
        """
        validate_list_not_empty(portfolio_data_list, "portfolio_data_list")

        portfolio_ids = []
        for portfolio_data in portfolio_data_list:
            try:
                edm_name = portfolio_data['edm_name']
                portfolio_name = portfolio_data['portfolio_name']
                portfolio_number = portfolio_data['portfolio_number']
                description = portfolio_data['description']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing value in create portfolio data: {e}"
                ) from e
            
            # Returns tuple of (portfolio_id, request_body) - we only need portfolio_id here
            portfolio_id, _ = self.create_portfolio(
                edm_name=edm_name,
                portfolio_name=portfolio_name,
                portfolio_number=portfolio_number,
                description=description
            )
            portfolio_ids.append(portfolio_id)

        return portfolio_ids


    def create_portfolio(
        self,
        edm_name: str,
        portfolio_name: str,
        portfolio_number: str = "",
        description: str = ""
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Create new portfolio in EDM.

        Args:
            edm_name: Name of EDM datasource
            portfolio_name: Name for new portfolio
            portfolio_number: Portfolio number; defaults to portfolio_name when
                empty and is truncated to 20 characters (default: "")
            description: Portfolio description; an auto-generated description is
                used when empty (default: "")

        Returns:
            Tuple of (portfolio_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")

        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if (len(edms) != 1):
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
            ) from e

        portfolios = self.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
        if (len(portfolios) > 0):
            raise IRPAPIError(f"{len(portfolios)} portfolios found with name {portfolio_name}, please use a unique name")

        if not portfolio_number:
            portfolio_number = portfolio_name
        if not description:
            description = "Portfolio created via API on " + datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S %Z")

        data = {
            "portfolioName": portfolio_name,
            "portfolioNumber": portfolio_number[:20],
            "description": description,
        }

        try:
            logger.info("Creating portfolio '%s' in exposure ID %s", portfolio_name, exposure_id)
            response = self.client.request('POST', CREATE_PORTFOLIO.format(exposureId=exposure_id), json=data)
            portfolio_id = extract_id_from_location_header(response, "portfolio creation")
            logger.info("Portfolio created — ID: %s", portfolio_id)
            return int(portfolio_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to create portfolio '{portfolio_name}' in exposure id '{exposure_id}': {e}")


    def submit_geohaz_jobs(self, geohaz_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Submit multiple geohaz jobs (geocoding and hazard operations).

        Args:
            geohaz_data_list: List of geohaz data dicts, each containing:
                - edm_name: str
                - portfolio_name: str
                - version: str
                - hazard_eq: bool
                - hazard_ws: bool

        Returns:
            List of job IDs

        Raises:
            IRPValidationError: If geohaz_data_list is empty or invalid
            IRPAPIError: If job submission fails or resources not found
        """
        validate_list_not_empty(geohaz_data_list, "geohaz_data_list")

        job_ids = []
        for geohaz_data in geohaz_data_list:
            try:
                edm_name = geohaz_data['edm_name']
                portfolio_name = geohaz_data['portfolio_name']
                version = geohaz_data['version']
                hazard_eq = geohaz_data['hazard_eq']
                hazard_ws = geohaz_data['hazard_ws']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing geohaz job data: {e}"
                ) from e

            # Returns tuple of (job_id, request_body) - we only need job_id here
            job_id, _ = self.submit_geohaz_job(
                portfolio_name=portfolio_name,
                edm_name=edm_name,
                version=version,
                hazard_eq=hazard_eq,
                hazard_ws=hazard_ws
            )
            job_ids.append(job_id)

        return job_ids
        

    def submit_geohaz_job(self,
                          portfolio_name: str,
                          edm_name: str,
                          version: str = "22.0",
                          hazard_eq: bool = False,
                          hazard_ws: bool = False,
                          geocode_layer_options: Optional[Dict[str, Any]] = None,
                          hazard_layer_options: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Execute geocoding and/or hazard operations on portfolio.

        Args:
            portfolio_name: Name of the portfolio
            edm_name: Name of the EDM containing the portfolio
            version: Geocode version (default: "22.0")
            hazard_eq: Enable earthquake hazard (default: False)
            hazard_ws: Enable windstorm hazard (default: False)
            geocode_layer_options: Geocode layer option overrides; a default
                set is used when None (default: None)
            hazard_layer_options: Hazard layer option overrides; a default set
                is used when None (default: None)

        Returns:
            Tuple of (job_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If workflow fails or times out
        """
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(edm_name, "edm_name")

        # Look up EDM to get exposure_id
        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if len(edms) != 1:
            raise IRPAPIError(f"Expected 1 EDM with name '{edm_name}', found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(f"Failed to extract exposure ID for EDM '{edm_name}': {e}") from e

        # Look up portfolio to get portfolio_uri and portfolio_id
        portfolios = self.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
        if len(portfolios) != 1:
            raise IRPAPIError(f"Expected 1 portfolio with name '{portfolio_name}', found {len(portfolios)}")
        try:
            portfolio_uri = portfolios[0]['uri']
            portfolio_id = portfolios[0]['portfolioId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(f"Failed to extract portfolio details for portfolio '{portfolio_name}': {e}") from e

        # Check if portfolio has locations to GeoHaz
        accounts = self.search_accounts_by_portfolio(exposure_id=exposure_id, portfolio_id=portfolio_id)
        if len(accounts) == 0:
            raise IRPAPIError(f"Portfolio '{portfolio_name}' does not have any Accounts/Locations to be GeoHaz'd")

        # Validate locations count
        try:
            locations_count = 0
            for account in accounts:
                locations_count += account['locationsCount']
                if locations_count > 0:
                    break
        except (KeyError, TypeError, IndexError) as e:
            raise IRPAPIError(f"Failed to validate locations count for portfolio '{portfolio_name}': {e}") from e

        if locations_count == 0:
            raise IRPAPIError(f"Portfolio '{portfolio_name}' has accounts but no locations to be GeoHaz'd")

        if geocode_layer_options is None:
            geocode_layer_options = {
                "aggregateTriggerEnabled": "true",
                "geoLicenseType": "0",
                "skipPrevGeocoded": False
            }

        if hazard_layer_options is None:
            hazard_layer_options = {
                "overrideUserDef": False,
                "skipPrevHazard": False
            }

        data = {
            "resourceUri": portfolio_uri,
            "resourceType": "portfolio",
            "settings": {
                "layers": [
                    {
                        "type": "geocode",
                        "name": "geocode",
                        "engineType": "RL",
                        "version": version,
                        "layerOptions": geocode_layer_options
                    }
                ]
            }
        }

        if hazard_eq:
            data['settings']['layers'].append(
                {
                    "type": "hazard",
                    "name": "earthquake",
                    "engineType": "RL",
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        if hazard_ws:
            data['settings']['layers'].append(
                {
                    "type": "hazard",
                    "name": "windstorm",
                    "engineType": "RL",
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        try:
            logger.info("Submitting GeoHaz job for portfolio '%s'", portfolio_name)
            response = self.client.request(
                'POST',
                GEOHAZ_PORTFOLIO,
                json=data
            )
            job_id = extract_id_from_location_header(response, "portfolio geohaz")
            logger.info("GeoHaz job submitted — job ID: %s", job_id)
            return int(job_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to execute geohaz for portfolio '{portfolio_uri}': {e}")
        
    
    def get_geohaz_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve geohaz job status by job ID.

        Args:
            job_id: Job ID

        Returns:
            Dict containing job status details

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_GEOHAZ_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get geohaz job status for job ID {job_id}: {e}")


    def poll_geohaz_job_to_completion(
        self,
        job_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll geohaz job until completion or timeout.

        Args:
            job_id: Job ID
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final job status details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job times out
            IRPAPIError: If polling fails
        """
        validate_positive_int(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            logger.info("Polling GeoHaz job ID %s", job_id)
            job_data = self.get_geohaz_job(job_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for job ID {job_id}: {e}"
                ) from e
            logger.info("GeoHaz job %s status: %s; progress: %s", job_id, status, progress)
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data

            if time.time() - start > timeout:
                logger.error("GeoHaz job %s timed out after %s seconds. Last status: %s", job_id, timeout, status)
                raise IRPJobError(
                    f"GeoHaz job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def poll_geohaz_job_batch_to_completion(
            self,
            job_ids: List[int],
            interval: int = 20,
            timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple geohaz jobs until all complete or timeout.

        Args:
            job_ids: List of job IDs
            interval: Polling interval in seconds (default: 20)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            List of final job status details for all jobs

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If jobs time out
            IRPAPIError: If polling fails
        """
        validate_list_not_empty(job_ids, "job_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            logger.info("Polling batch GeoHaz job IDs: %s", ",".join(str(item) for item in job_ids))

            all_completed = False
            all_jobs = []
            for job_id in job_ids:
                workflow_response = self.get_geohaz_job(job_id)
                all_jobs.append(workflow_response)
                try:
                    status = workflow_response['status']
                except (KeyError, TypeError) as e:
                    raise IRPAPIError(
                        f"Missing 'status' in workflow response for job ID {job_id}: {e}"
                    ) from e
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_jobs = []
                    break
                all_completed = True

            if all_completed:
                return all_jobs
            
            if time.time() - start > timeout:
                logger.error("Batch GeoHaz jobs timed out after %s seconds", timeout)
                raise IRPJobError(
                    f"Batch geohaz jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)
