# API Reference

_This file is generated from source docstrings by `docs/generate_api_docs.py`. Do not edit by hand — run `python docs/generate_api_docs.py` to regenerate._

Python client library for Moody's Risk Modeler API.

The single entry point is ``IRPClient``, which holds one HTTP client and exposes a manager per functional area; reach operations through those managers.

**Managers (``client.<name>``):**

edm, portfolio, mri_import, treaty, analysis, risk_data_job, rdm, import_job, export_job, reference_data, and (optional) databridge.

Name-based interface: high-level methods accept human-readable names (EDM names, portfolio names, profile names, treaty names) and resolve them to IDs internally.

S3 transfers for import/export staging are handled transparently by the relevant managers — there is no need to hand-roll boto3.

Data Bridge (SQL Server) support is optional: ``client.databridge`` exists only when the ``[databridge]`` extra and its ODBC driver are installed.

**Pointers:**

- Cross-cutting workflow contract, including terminal-status handling → ``client.py``.
- Domain concepts → each area's module docstring (e.g. ``analysis.py``, ``edm.py``, ``rdm.py``, ``treaty.py``).

## Table of Contents

- [`irp_integration.client`](#irp_integrationclient)
  - [Client](#class-client)
- [`irp_integration.edm`](#irp_integrationedm)
  - [EDMManager](#class-edmmanager)
- [`irp_integration.portfolio`](#irp_integrationportfolio)
  - [PortfolioManager](#class-portfoliomanager)
- [`irp_integration.mri_import`](#irp_integrationmri_import)
  - [MRIImportManager](#class-mriimportmanager)
- [`irp_integration.treaty`](#irp_integrationtreaty)
  - [TreatyManager](#class-treatymanager)
- [`irp_integration.analysis`](#irp_integrationanalysis)
  - [AnalysisManager](#class-analysismanager)
- [`irp_integration.rdm`](#irp_integrationrdm)
  - [RDMManager](#class-rdmmanager)
- [`irp_integration.risk_data_job`](#irp_integrationrisk_data_job)
  - [RiskDataJobManager](#class-riskdatajobmanager)
- [`irp_integration.import_job`](#irp_integrationimport_job)
  - [ImportJobManager](#class-importjobmanager)
- [`irp_integration.export_job`](#irp_integrationexport_job)
  - [ExportJobManager](#class-exportjobmanager)
- [`irp_integration.s3`](#irp_integrations3)
  - [S3Manager](#class-s3manager)
- [`irp_integration.reference_data`](#irp_integrationreference_data)
  - [ReferenceDataManager](#class-referencedatamanager)
- [`irp_integration.databridge`](#irp_integrationdatabridge)
  - [ExpressionTemplate](#class-expressiontemplate)
  - [DataBridgeManager](#class-databridgemanager)
- [`irp_integration.exceptions`](#irp_integrationexceptions)
  - [IRPIntegrationError](#class-irpintegrationerror)
  - [IRPAPIError](#class-irpapierror)
  - [IRPAuthenticationError](#class-irpauthenticationerror)
  - [IRPValidationError](#class-irpvalidationerror)
  - [IRPWorkflowError](#class-irpworkflowerror)
  - [IRPReferenceDataError](#class-irpreferencedataerror)
  - [IRPFileError](#class-irpfileerror)
  - [IRPJobError](#class-irpjoberror)
  - [IRPDataBridgeError](#class-irpdatabridgeerror)
  - [IRPDataBridgeConnectionError](#class-irpdatabridgeconnectionerror)
  - [IRPDataBridgeQueryError](#class-irpdatabridgequeryerror)
- [`irp_integration.validators`](#irp_integrationvalidators)
- [`irp_integration.utils`](#irp_integrationutils)
- [`irp_integration.constants`](#irp_integrationconstants)

---

## `irp_integration.client`

Client for IRP Integration API requests.

HTTP transport plus the cross-cutting contracts every manager relies on. This module is the authoritative home for those contracts; other modules point here rather than restating them.

**Async workflow model:**

Most write operations are asynchronous: submit a request, receive a ``201``/``202`` with a ``Location`` header naming the workflow, then poll that workflow until it reaches a terminal status. Building blocks:

- ``execute_workflow(method, path, ...)`` — submit and poll in one call.
- ``poll_workflow(url)`` — poll a workflow by its ``Location`` URL.
- ``poll_workflow_to_completion(id)`` — poll a workflow by ID.
- ``poll_workflow_batch_to_completion(ids)`` — poll many workflows at once.

**Terminal status is not success:**

``WORKFLOW_COMPLETED_STATUSES`` is ``FINISHED``, ``FAILED``, and ``CANCELLED``. Polling returns as soon as a workflow reaches *any* of these — including ``FAILED`` and ``CANCELLED``. A returned result therefore signals only that the workflow is done, not that it succeeded; the caller must inspect the returned ``status``.

**Retries:**

Retries are built into the underlying session — 5 attempts with exponential backoff for ``429`` and ``5xx`` responses, across all HTTP methods. Do not add another retry layer on top of these calls.

**Auth/config:**

The API base URL (``RISK_MODELER_BASE_URL``) and resource group (``RISK_MODELER_RESOURCE_GROUP_ID``) are always required. Two authentication strategies are supported, selected automatically in ``Client.__init__`` from which environment variables are populated:

- **API key** (default / preserves existing behavior): if ``RISK_MODELER_API_KEY`` is set, it is sent verbatim in the ``Authorization`` header.
- **Bearer login**: if the API key is absent but ``RISK_MODELER_TENANT_NAME``, ``RISK_MODELER_USERNAME``, and ``RISK_MODELER_PASSWORD`` are all set, the client logs in at construction to obtain a short-lived (1-hour) bearer token and sends ``Authorization: Bearer {accessToken}``.

The API key takes precedence when both option sets are present. Bearer tokens are refreshed reactively: a ``401`` triggers a single re-login with the stored credentials and one retry of the request. ``__init__`` raises if neither complete option set is configured.

### `class Client`

Client for Moody's Risk Modeler API.

#### `__init__`

```python
def __init__(self)
```

Initialize API client with credentials from environment.

Two authentication strategies are supported, selected automatically
from which environment variables are populated (see the module
docstring for details):

    - **API key** (default): ``RISK_MODELER_API_KEY`` set.
    - **Bearer login**: API key absent and all of
      ``RISK_MODELER_TENANT_NAME``, ``RISK_MODELER_USERNAME``, and
      ``RISK_MODELER_PASSWORD`` set. The client logs in immediately to
      fetch the initial token.

The API key takes precedence when both option sets are present. The
chosen strategy is exposed as ``self.auth_mode`` (``'apikey'`` or
``'bearer'``); it is set once here and should be treated as read-only.

**Environment variables:**
> RISK_MODELER_BASE_URL: API base URL (always required)
> RISK_MODELER_RESOURCE_GROUP_ID: Resource group ID (always required)
> RISK_MODELER_API_KEY: API authentication key (API-key strategy)
> RISK_MODELER_TENANT_NAME: Tenant name (bearer strategy)
> RISK_MODELER_USERNAME: Username (bearer strategy)
> RISK_MODELER_PASSWORD: Password (bearer strategy)

**Raises:**
 - **IRPAPIError:**  If required configuration is missing or no complete
   authentication strategy is configured
 - **IRPAuthenticationError:**  If the initial bearer login fails

#### `request`

```python
def request(
    self,
    method: str,
    path: str,
    *,
    full_url: Optional[str] = None,
    base_url: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Union[Dict[str, Any], List[Any], NoneType] = None,
    headers: Dict[str, str] = {},
    timeout: Optional[int] = None,
    stream: bool = False
) -> requests.models.Response
```

Make HTTP request to API.

**Arguments:**
 - **method:**  HTTP method (GET, POST, PUT, DELETE, etc.)
 - **path:**  API path (e.g., '/api/v1/datasources')
 - **full_url:**  Full URL (overrides path/base_url if provided)
 - **base_url:**  Base URL (overrides default if provided)
 - **params:**  Query parameters
 - **json:**  JSON request body
 - **headers:**  Additional headers
 - **timeout:**  Request timeout in seconds
 - **stream:**  Enable streaming response

**Returns:**
> HTTP response object

**Raises:**
 - **IRPAPIError:**  If the HTTP request fails
 - **IRPAuthenticationError:**  In bearer mode, if a ``401`` persists after
   a re-login and retry

#### `get_workflow`

```python
def get_workflow(self, workflow_id: int) -> Dict[str, Any]
```

Retrieve workflow status by workflow ID.

**Arguments:**
 - **workflow_id:**  Workflow ID

**Returns:**
> Dict containing workflow status details

**Raises:**
 - **IRPValidationError:**  If workflow_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_workflow_to_completion`

```python
def poll_workflow_to_completion(
    self,
    workflow_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll workflow until completion or timeout.

Returns on any terminal status (FINISHED, FAILED, or CANCELLED) — the
caller must inspect the returned ``status``; see "Terminal status is not
success" in the module docstring.

**Arguments:**
 - **workflow_id:**  Workflow ID
 - **interval:**  Polling interval in seconds
 - **timeout:**  Maximum timeout in seconds

**Returns:**
> Dict containing the final workflow status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If the workflow times out
 - **IRPAPIError:**  If a status request fails

#### `poll_workflow`

```python
def poll_workflow(
    self,
    workflow_url: str,
    interval: int = 10,
    timeout: int = 600000
) -> requests.models.Response
```

Poll workflow until completion or timeout.

**Arguments:**
 - **workflow_url:**  Full URL to workflow endpoint
 - **interval:**  Polling interval in seconds
 - **timeout:**  Maximum timeout in seconds

**Returns:**
> Final workflow response

**Raises:**
 - **IRPValidationError:**  If workflow_url is invalid
 - **IRPWorkflowError:**  If workflow times out

#### `poll_workflow_batch_to_completion`

```python
def poll_workflow_batch_to_completion(
    self,
    workflow_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> requests.models.Response
```

Poll multiple workflows until all complete or timeout.

**Arguments:**
 - **workflow_ids:**  List of workflow IDs to poll
 - **interval:**  Polling interval in seconds
 - **timeout:**  Maximum timeout in seconds

**Returns:**
> Response with all workflows combined

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPWorkflowError:**  If workflows time out

#### `execute_workflow`

```python
def execute_workflow(
    self,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json: Union[Dict[str, Any], List[Any], NoneType] = None,
    headers: Dict[str, str] = {},
    timeout: Optional[int] = None,
    stream: bool = False
) -> requests.models.Response
```

Execute workflow: submit request and poll until completion.

This is a convenience method that combines request submission
with automatic workflow polling.

**Arguments:**
 - **method:**  HTTP method (POST, DELETE, etc.)
 - **path:**  API path
 - **params:**  Query parameters
 - **json:**  JSON request body
 - **headers:**  Additional headers
 - **timeout:**  Request timeout in seconds
 - **stream:**  Enable streaming response

**Returns:**
> Final workflow response after completion

**Raises:**
 - **IRPAPIError:**  If request fails
 - **IRPWorkflowError:**  If workflow times out

---

## `irp_integration.edm`

EDM (Exposure Data Management) operations.

Handles datasource creation, duplication, deletion, and associated data retrieval (cedants, LOBs).

### `class EDMManager`

Manager for EDM (Exposure Data Management) operations.

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    portfolio_manager: Optional[Any] = None,
    analysis_manager: Optional[Any] = None,
    risk_data_job_manager: Optional[Any] = None
)
```

Initialize EDM manager.

**Arguments:**
 - **client:**  IRP API client instance
 - **portfolio_manager:**  Optional PortfolioManager instance
 - **analysis_manager:**  Optional AnalysisManager instance
 - **risk_data_job_manager:**  Optional RiskDataJobManager instance

#### `validate_unique_edms`

```python
def validate_unique_edms(self, edm_names: List[str]) -> None
```

Validate that EDM names are unique (don't already exist).

**Arguments:**
 - **edm_names:**  List of EDM names to validate

**Raises:**
 - **IRPAPIError:**  If any EDM names already exist

#### `submit_create_edm_jobs`

```python
def submit_create_edm_jobs(self, edm_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple EDM creation jobs.

**Arguments:**
 - **edm_data_list:**  List of EDM data dicts, each containing:
   - server_name: str
   - edm_name: str

**Returns:**
> List of job IDs

**Raises:**
 - **IRPValidationError:**  If edm_data_list is empty or invalid
 - **IRPAPIError:**  If EDM creation fails or duplicate names exist

#### `search_database_servers`

```python
def search_database_servers(self, filter: str = '') -> List[Dict[str, Any]]
```

Search database servers.

**Arguments:**
 - **filter:**  Optional filter string for server names

**Returns:**
> List of database server dicts

#### `search_exposure_sets`

```python
def search_exposure_sets(self, filter: str = '') -> List[Dict[str, Any]]
```

Search exposure sets.

**Arguments:**
 - **filter:**  Optional filter string for exposure set names

**Returns:**
> List of exposure set dicts

#### `create_exposure_set`

```python
def create_exposure_set(self, name: str) -> int
```

Create a new exposure set.

**Arguments:**
 - **name:**  Name of the exposure set

**Returns:**
> The exposure set ID

#### `search_edms`

```python
def search_edms(
    self,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search EDMs (exposures).

**Arguments:**
 - **filter:**  Optional filter string for EDM names
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of EDM dictionaries

#### `search_edms_paginated`

```python
def search_edms_paginated(self, filter: str = '') -> List[Dict[str, Any]]
```

Search all EDMs with automatic pagination.

Fetches all pages of results matching the filter criteria.

**Arguments:**
 - **filter:**  Optional filter string for EDM names

**Returns:**
> Complete list of all matching EDMs across all pages

#### `submit_create_edm_job`

```python
def submit_create_edm_job(
    self,
    edm_name: str,
    server_name: str = 'databridge-1'
) -> Tuple[int, Dict[str, Any]]
```

Submit job to create a new EDM (exposure).

**Arguments:**
 - **edm_name:**  Name of the EDM
 - **server_name:**  Name of the database server (default: "databridge-1")

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

#### `submit_upgrade_edm_data_version_jobs`

```python
def submit_upgrade_edm_data_version_jobs(self, edm_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple EDM data version upgrade jobs.

**Arguments:**
 - **edm_data_list:**  List of EDM upgrade data dicts, each containing:
   - edm_name: str
   - edm_version: str

**Returns:**
> List of job IDs

**Raises:**
 - **IRPValidationError:**  If edm_data_list is empty or invalid
 - **IRPAPIError:**  If upgrade submission fails or EDM not found

#### `submit_upgrade_edm_data_version_job`

```python
def submit_upgrade_edm_data_version_job(self, edm_name: str, edm_version: str) -> Tuple[int, Dict[str, Any]]
```

Submit job to upgrade EDM data version.

**Arguments:**
 - **edm_name:**  Name of the EDM to upgrade
 - **edm_version:**  Target EDM data version (e.g., "22")

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If EDM not found or upgrade fails

#### `poll_data_version_upgrade_job_batch_to_completion`

```python
def poll_data_version_upgrade_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple EDM data version upgrade jobs until all complete or timeout.

**Arguments:**
 - **job_ids:**  List of job IDs
 - **interval:**  Polling interval in seconds (default: 20)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> List of final job status details for all jobs

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If jobs time out
 - **IRPAPIError:**  If polling fails

#### `delete_edm`

```python
def delete_edm(self, edm_name: str) -> Dict[str, Any]
```

Delete an EDM and all its associated analyses.

**Arguments:**
 - **edm_name:**  Name of EDM to delete

**Returns:**
> Dict containing final delete job status

**Raises:**
 - **IRPValidationError:**  If edm_name is invalid
 - **IRPAPIError:**  If EDM not found or deletion fails

#### `submit_delete_edm_job`

```python
def submit_delete_edm_job(self, exposure_id: int) -> int
```

Submit job to delete an EDM (exposure).

**Arguments:**
 - **exposure_id:**  ID of the exposure (EDM)

**Returns:**
> The job ID

#### `get_cedants_by_edm`

```python
def get_cedants_by_edm(self, exposure_id: int) -> List[Dict[str, Any]]
```

Retrieve cedants for an EDM.

**Arguments:**
 - **exposure_id:**  Exposure ID

**Returns:**
> List of cedant data

**Raises:**
 - **IRPValidationError:**  If exposure_id is invalid
 - **IRPAPIError:**  If request fails

#### `get_lobs_by_edm`

```python
def get_lobs_by_edm(self, exposure_id: int) -> List[Dict[str, Any]]
```

Retrieve lines of business (LOBs) for an EDM.

**Arguments:**
 - **exposure_id:**  Exposure ID

**Returns:**
> List of LOB dicts

**Raises:**
 - **IRPValidationError:**  If exposure_id is invalid
 - **IRPAPIError:**  If request fails

#### `submit_edm_import_job`

```python
def submit_edm_import_job(
    self,
    edm_name: str,
    edm_file_path: str,
    server_name: str = 'sql-instance-1'
) -> Tuple[int, Dict[str, Any]]
```

Submit EDM import job with S3 file upload.

This method handles the complete EDM import workflow:
1. Create import folder (get S3 credentials)
2. Upload EDM .bak file to S3
3. Create or get existing exposure set
4. Submit import job

**Arguments:**
 - **edm_name:**  Name for the EDM
 - **edm_file_path:**  Path to the .bak file to import
 - **server_name:**  Database server name (default: "sql-instance-1")

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPFileError:**  If file upload fails
 - **IRPAPIError:**  If API calls fail

---

## `irp_integration.portfolio`

Portfolio management operations.

Handles portfolio creation, retrieval, and geocoding/hazard operations.

### `class PortfolioManager`

Manager for portfolio operations.

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    edm_manager: Optional[Any] = None
)
```

Initialize portfolio manager.

**Arguments:**
 - **client:**  IRP API client instance
 - **edm_manager:**  Optional EDMManager instance

#### `get_portfolio_by_id`

```python
def get_portfolio_by_id(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]
```

Retrieve portfolio details by portfolio ID.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **portfolio_id:**  Portfolio ID

**Returns:**
> Dict containing portfolio details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails

#### `get_portfolio_metadata`

```python
def get_portfolio_metadata(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]
```

Retrieve portfolio metadata by portfolio ID.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **portfolio_id:**  Portfolio ID

**Returns:**
> Dict containing portfolio metadata details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails

#### `search_portfolios`

```python
def search_portfolios(
    self,
    exposure_id: int,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search portfolios within an exposure.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **filter:**  Optional filter string for portfolio names
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of portfolio dictionaries

#### `search_portfolios_paginated`

```python
def search_portfolios_paginated(self, exposure_id: int, filter: str = '') -> List[Dict[str, Any]]
```

Search all portfolios within an exposure with automatic pagination.

Fetches all pages of results matching the filter criteria.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **filter:**  Optional filter string for portfolio names

**Returns:**
> Complete list of all matching portfolios across all pages

#### `search_accounts_by_portfolio`

```python
def search_accounts_by_portfolio(self, exposure_id: int, portfolio_id: int) -> List[Dict[str, Any]]
```

Retrieve accounts within a portfolio.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **portfolio_id:**  Portfolio ID

**Returns:**
> List of account dicts

#### `create_portfolios`

```python
def create_portfolios(self, portfolio_data_list: List[Dict[str, Any]]) -> List[int]
```

Create multiple portfolios.

**Arguments:**
 - **portfolio_data_list:**  List of portfolio data dicts, each containing:
   - edm_name: str
   - portfolio_name: str
   - portfolio_number: str
   - description: str

**Returns:**
> List of portfolio IDs

**Raises:**
 - **IRPValidationError:**  If portfolio_data_list is empty or invalid
 - **IRPAPIError:**  If portfolio creation fails or duplicate names exist

#### `create_portfolio`

```python
def create_portfolio(
    self,
    edm_name: str,
    portfolio_name: str,
    portfolio_number: str = '',
    description: str = ''
) -> Tuple[int, Dict[str, Any]]
```

Create new portfolio in EDM.

**Arguments:**
 - **edm_name:**  Name of EDM datasource
 - **portfolio_name:**  Name for new portfolio
 - **portfolio_number:**  Portfolio number; defaults to portfolio_name when
   empty and is truncated to 20 characters (default: "")
 - **description:**  Portfolio description; an auto-generated description is
   used when empty (default: "")

**Returns:**
> Tuple of (portfolio_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If request fails

#### `submit_geohaz_jobs`

```python
def submit_geohaz_jobs(self, geohaz_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple geohaz jobs (geocoding and hazard operations).

**Arguments:**
 - **geohaz_data_list:**  List of geohaz data dicts, each containing:
   - edm_name: str
   - portfolio_name: str
   - version: str
   - hazard_eq: bool
   - hazard_ws: bool

**Returns:**
> List of job IDs

**Raises:**
 - **IRPValidationError:**  If geohaz_data_list is empty or invalid
 - **IRPAPIError:**  If job submission fails or resources not found

#### `submit_geohaz_job`

```python
def submit_geohaz_job(
    self,
    portfolio_name: str,
    edm_name: str,
    version: str = '22.0',
    hazard_eq: bool = False,
    hazard_ws: bool = False,
    geocode_layer_options: Optional[Dict[str, Any]] = None,
    hazard_layer_options: Optional[Dict[str, Any]] = None
) -> Tuple[int, Dict[str, Any]]
```

Execute geocoding and/or hazard operations on portfolio.

**Arguments:**
 - **portfolio_name:**  Name of the portfolio
 - **edm_name:**  Name of the EDM containing the portfolio
 - **version:**  Geocode version (default: "22.0")
 - **hazard_eq:**  Enable earthquake hazard (default: False)
 - **hazard_ws:**  Enable windstorm hazard (default: False)
 - **geocode_layer_options:**  Geocode layer option overrides; a default
   set is used when None (default: None)
 - **hazard_layer_options:**  Hazard layer option overrides; a default set
   is used when None (default: None)

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If workflow fails or times out

#### `get_geohaz_job`

```python
def get_geohaz_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve geohaz job status by job ID.

**Arguments:**
 - **job_id:**  Job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_geohaz_job_to_completion`

```python
def poll_geohaz_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll geohaz job until completion or timeout.

**Arguments:**
 - **job_id:**  Job ID
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job times out
 - **IRPAPIError:**  If polling fails

#### `poll_geohaz_job_batch_to_completion`

```python
def poll_geohaz_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple geohaz jobs until all complete or timeout.

**Arguments:**
 - **job_ids:**  List of job IDs
 - **interval:**  Polling interval in seconds (default: 20)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> List of final job status details for all jobs

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If jobs time out
 - **IRPAPIError:**  If polling fails

---

## `irp_integration.mri_import`

MRI Import Manager for IRP Integration.

Handles Multi-Risk Insurance (MRI) data imports via the Platform Import API. Files are uploaded to S3 and import jobs are submitted through the /platform/import/v1 endpoints.

### `class MRIImportManager`

Manager for MRI import operations.

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    edm_manager: Optional[Any] = None,
    portfolio_manager: Optional[Any] = None
)
```

Initialize MRI Import Manager.

**Arguments:**
 - **client:**  Client instance for API requests
 - **edm_manager:**  Optional EDMManager instance
 - **portfolio_manager:**  Optional PortfolioManager instance

#### `submit_mri_import_job`

```python
def submit_mri_import_job(
    self,
    edm_name: str,
    portfolio_name: str,
    accounts_file_path: str,
    locations_file_path: str,
    mapping_file_path: Optional[str] = None,
    delimiter: str = 'TAB'
) -> Tuple[int, Dict[str, Any]]
```

Submit an MRI import job via the Platform Import API.

This method handles the complete MRI import workflow:
1. Look up EDM and portfolio
2. Create import folder (get S3 credentials)
3. Upload accounts, locations, and optionally mapping files to S3
4. Submit import job

**Arguments:**
 - **edm_name:**  Target EDM name
 - **portfolio_name:**  Target portfolio name within the EDM
 - **accounts_file_path:**  Path to accounts CSV file
 - **locations_file_path:**  Path to locations CSV file
 - **mapping_file_path:**  Optional path to .mff mapping file
 - **delimiter:**  File delimiter (default: "TAB")

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPFileError:**  If file upload fails
 - **IRPAPIError:**  If any API call fails

---

## `irp_integration.treaty`

Treaty Manager for IRP Integration.

Handles treaty-related operations including creation, retrieval, and Line of Business (LOB) assignments.

### `class TreatyManager`

Manager for treaty operations.

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    edm_manager=None,
    reference_data_manager=None
)
```

Initialize Treaty Manager.

**Arguments:**
 - **client:**  Client instance for API requests
 - **edm_manager:**  Optional EDMManager instance (lazy-loaded if None)
 - **reference_data_manager:**  Optional ReferenceDataManager instance (lazy-loaded if None)

#### `search_treaties`

```python
def search_treaties(
    self,
    exposure_id: int,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search treaties for a given exposure ID.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **filter:**  Optional filter string
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of treaty dictionaries

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If API request fails

#### `search_treaties_paginated`

```python
def search_treaties_paginated(self, exposure_id: int, filter: str = '') -> List[Dict[str, Any]]
```

Search all treaties for a given exposure ID with automatic pagination.

Fetches all pages of results matching the filter criteria.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **filter:**  Optional filter string

**Returns:**
> Complete list of all matching treaties across all pages

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If API request fails

#### `create_treaties`

```python
def create_treaties(self, treaty_data_list: List[Dict[str, Any]]) -> List[int]
```

Create multiple treaties.

**Arguments:**
 - **treaty_data_list:**  List of treaty data dicts, each containing all required treaty fields

**Returns:**
> List of treaty IDs

**Raises:**
 - **IRPValidationError:**  If treaty_data_list is empty or invalid
 - **IRPAPIError:**  If treaty creation fails or EDM not found

#### `create_treaty`

```python
def create_treaty(
    self,
    edm_name: str,
    treaty_name: str,
    treaty_number: str,
    treaty_type: str,
    per_risk_limit: float,
    occurrence_limit: float,
    attachment_point: float,
    inception_date: str,
    expiration_date: str,
    currency_name: str,
    attachment_basis: str,
    attachment_level: str,
    pct_covered: float,
    pct_placed: float,
    pct_share: float,
    pct_retention: float,
    premium: float,
    num_reinstatements: int,
    pct_reinstatement_charge: float,
    aggregate_limit: float,
    aggregate_deductible: float,
    priority: int
) -> Tuple[int, Dict[str, Any]]
```

Create a treaty with provided parameters.

**Arguments:**
 - **edm_name:**  EDM name to create the treaty in
 - **treaty_name:**  Treaty name
 - **treaty_number:**  Treaty number (max 20 chars)
 - **treaty_type:**  Treaty type (must be in TREATY_TYPES)
 - **per_risk_limit:**  Per risk limit amount
 - **occurrence_limit:**  Occurrence limit amount
 - **attachment_point:**  Attachment point amount
 - **inception_date:**  Inception date (ISO format)
 - **expiration_date:**  Expiration date (ISO format)
 - **currency_name:**  Currency name (e.g., "US Dollar")
 - **attachment_basis:**  Attachment basis (must be in TREATY_ATTACHMENT_BASES)
 - **attachment_level:**  Attachment level (must be in TREATY_ATTACHMENT_LEVELS)
 - **pct_covered:**  Percent covered
 - **pct_placed:**  Percent placed
 - **pct_share:**  Percent share
 - **pct_retention:**  Percent retention
 - **premium:**  Premium amount
 - **num_reinstatements:**  Number of reinstatements
 - **pct_reinstatement_charge:**  Percent reinstatement charge
 - **aggregate_limit:**  Aggregate limit amount
 - **aggregate_deductible:**  Aggregate deductible amount
 - **priority:**  Priority

**Returns:**
> Tuple of (treaty_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If treaty creation fails or EDM not found

#### `create_treaty_lob`

```python
def create_treaty_lob(self, exposure_id: int, treaty_id: int, lob_id: int, lobName: str) -> int
```

Create a Line of Business (LOB) for a treaty.

**Arguments:**
 - **exposure_id:**  Exposure ID
 - **treaty_id:**  Treaty ID
 - **lob_id:**  LOB ID
 - **lobName:**  LOB Name

**Returns:**
> LOB ID of the created LOB

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If LOB creation fails

---

## `irp_integration.analysis`

Analysis management operations.

Handles portfolio analysis submission, job tracking, and analysis group creation.

### `class AnalysisManager`

Manager for analysis operations.

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    reference_data_manager: Optional[Any] = None,
    treaty_manager: Optional[Any] = None,
    edm_manager: Optional[Any] = None,
    portfolio_manager: Optional[Any] = None
)
```

Initialize analysis manager.

**Arguments:**
 - **client:**  IRP API client instance
 - **reference_data_manager:**  Optional ReferenceDataManager instance
 - **treaty_manager:**  Optional TreatyManager instance
 - **edm_manager:**  Optional EDMManager instance
 - **portfolio_manager:**  Optional PortfolioManager instance

#### `get_analysis_by_id`

```python
def get_analysis_by_id(self, analysis_id: int) -> Dict[str, Any]
```

Retrieve analysis by ID.

**Arguments:**
 - **analysis_id:**  Analysis ID

**Returns:**
> Dict containing analysis details

**Raises:**
 - **IRPValidationError:**  If analysis_id is invalid
 - **IRPAPIError:**  If request fails

#### `submit_portfolio_analysis_jobs`

```python
def submit_portfolio_analysis_jobs(self, analysis_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple portfolio analysis jobs.

**Arguments:**
 - **analysis_data_list:**  List of analysis job data dicts, each containing:
   - edm_name: str
   - portfolio_name: str
   - job_name: str
   - analysis_profile_name: str
   - output_profile_name: str
   - event_rate_scheme_name: str
   - treaty_names: List[str]
   - tag_names: List[str]

**Returns:**
> List of job IDs

**Raises:**
 - **IRPValidationError:**  If analysis_data_list is empty or invalid
 - **IRPAPIError:**  If analysis submission fails or duplicate analysis names exist

#### `submit_portfolio_analysis_job`

```python
def submit_portfolio_analysis_job(
    self,
    edm_name: str,
    portfolio_name: str,
    job_name: str,
    analysis_profile_name: str,
    output_profile_name: str,
    event_rate_scheme_name: str,
    treaty_names: List[str],
    tag_names: List[str],
    currency: Optional[Dict[str, str]] = None,
    skip_duplicate_check: bool = False,
    franchise_deductible: bool = False,
    min_loss_threshold: float = 1.0,
    treat_construction_occupancy_as_unknown: bool = True,
    num_max_loss_event: int = 1
) -> Tuple[int, Dict[str, Any]]
```

Submit portfolio analysis job (submits but doesn't wait).

**Arguments:**
 - **edm_name:**  Name of the EDM (exposure database)
 - **portfolio_name:**  Name of the portfolio to analyze
 - **job_name:**  Name for analysis job (must be unique)
 - **analysis_profile_name:**  Model profile name
 - **output_profile_name:**  Output profile name
 - **event_rate_scheme_name:**  Event rate scheme name (required for DLM, optional for HD)
 - **treaty_names:**  List of treaty names to apply
 - **tag_names:**  List of tag names to apply
 - **currency:**  Optional currency configuration
 - **skip_duplicate_check:**  Skip checking if analysis name already exists (for batch operations)
 - **franchise_deductible:**  Whether to apply franchise deductible (default: False)
 - **min_loss_threshold:**  Minimum loss threshold value (default: 0)
 - **treat_construction_occupancy_as_unknown:**  Treat construction/occupancy as unknown (default: True)
 - **num_max_loss_event:**  Number of max loss events to include (default: 1)

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If request fails or EDM/portfolio not found

#### `submit_analysis_grouping_jobs`

```python
def submit_analysis_grouping_jobs(
    self,
    grouping_data_list: List[Dict[str, Any]],
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> List[int]
```

Submit multiple analysis grouping jobs.

**Arguments:**
 - **grouping_data_list:**  List of grouping data dicts, each containing:
   - group_name: str
   - analysis_names: List[str] (can include both analysis names and group names)
 - **analysis_edm_map:**  Optional mapping of analysis names to EDM names.
   Used to look up analyses by name + EDM (since analysis names are only
   unique within an EDM). If not provided, lookups use name only.
 - **group_names:**  Optional set of known group names. Items in this set are
   looked up as groups (by name only), all others are looked up as
   analyses (by name + EDM if mapping provided).
 - **skip_missing:**  If True (default), skip analyses/groups that don't exist.
   Jobs where all items are missing will be skipped entirely.

**Returns:**
> List of job IDs (excludes skipped jobs)

**Raises:**
 - **IRPValidationError:**  If grouping_data_list is empty or invalid
 - **IRPAPIError:**  If grouping submission fails or analysis names not found

#### `build_region_peril_simulation_set`

```python
def build_region_peril_simulation_set(self, analysis_ids: List[int]) -> List[Dict[str, Any]]
```

Build regionPerilSimulationSet from analysis/group IDs for grouping requests.

This method fetches regions for each analysis/group and builds the required
regionPerilSimulationSet structure. This is required for mixed ELT/PLT grouping
(combining DLM and HD analyses/groups).

For ELT framework (DLM):
    - eventRateSchemeId comes from regions response (rateSchemeId)
    - simulationSetId is looked up from SimulationSet table using eventRateSchemeId

For PLT framework (HD):
    - eventRateSchemeId = 0 (always zero for PLT in grouping requests)
    - simulationSetId = petId from regions response

For Compound Perils (subPeril contains "+"):
    - If ALL analyses have compound perils -> return empty array
    - If SOME analyses have compound perils -> all analyses contribute normally
    - The API handles event correlation internally when array is empty
    - Examples: "Surge + Wind", "Tornado + Hail + Wind"

**Arguments:**
 - **analysis_ids:**  List of analysis or group IDs to include

**Returns:**
> List of region/peril simulation set entries, each containing:
>     - engineVersion: Engine version (e.g., "RL23", "HDv2.0")
>     - eventRateSchemeId: Event rate scheme ID (0 for PLT)
>     - modelRegionCode: Model region code (subRegion from regions)
>     - modelVersion: Model version (looked up from SoftwareModelVersionMap)
>     - perilCode: Peril code (e.g., "EQ", "WS", "FL")
>     - regionCode: Region code (e.g., "NA", "US")
>     - simulationPeriods: Number of simulation periods
>     - simulationSetId: Simulation set ID
> 
> Returns empty list if ALL analyses have compound perils.

**Raises:**
 - **IRPAPIError:**  If any API calls fail

#### `submit_analysis_grouping_job`

```python
def submit_analysis_grouping_job(
    self,
    group_name: str,
    analysis_names: List[str],
    simulate_to_plt: bool = False,
    num_simulations: int = 50000,
    propagate_detailed_losses: bool = False,
    reporting_window_start: str = '01/01/2021',
    simulation_window_start: str = '01/01/2021',
    simulation_window_end: str = '12/31/2021',
    region_peril_simulation_set: Optional[List[Dict[str, Any]]] = None,
    description: str = '',
    currency: Optional[Dict[str, str]] = None,
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> Dict[str, Any]
```

Submit analysis grouping job.

**Arguments:**
 - **group_name:**  Name for analysis group
 - **analysis_names:**  List of names to include in the group (can be analyses or groups)
 - **simulate_to_plt:**  Whether to simulate to PLT (default: True)
 - **num_simulations:**  Number of simulations (default: 50000)
 - **propagate_detailed_losses:**  Whether to propagate detailed losses (default: False)
 - **reporting_window_start:**  Reporting window start date (default: "01/01/2021")
 - **simulation_window_start:**  Simulation window start date (default: "01/01/2021")
 - **simulation_window_end:**  Simulation window end date (default: "12/31/2021")
 - **region_peril_simulation_set:**  Region/peril simulation set (default: None)
 - **description:**  Group description (default: "")
 - **currency:**  Currency configuration (default: None, uses system default)
 - **analysis_edm_map:**  Optional mapping of analysis names to EDM names.
   Used to look up analyses by name + EDM (since analysis names are only
   unique within an EDM). If not provided, lookups use name only.
 - **group_names:**  Optional set of known group names. Items in this set are
   looked up as groups (by name only), all others are looked up as
   analyses (by name + EDM if mapping provided).
 - **skip_missing:**  If True (default), skip analyses/groups that don't exist
   instead of raising an error. If all items are missing, returns
   a result with job_id=None and skipped=True.

**Returns:**
> Dict containing:
>     - job_id: Analysis group job ID (int), or None if skipped
>     - skipped: True if job was skipped (all analyses missing)
>     - skipped_items: List of item names that were not found and skipped
>     - included_items: List of item names that were found and included

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If request fails, or if skip_missing=False and items not found

#### `get_analysis_grouping_job`

```python
def get_analysis_grouping_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve analysis grouping job status by job ID.

**Arguments:**
 - **job_id:**  Job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_analysis_grouping_job_to_completion`

```python
def poll_analysis_grouping_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll analysis grouping job until completion or timeout.

**Arguments:**
 - **job_id:**  Job ID
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job times out
 - **IRPAPIError:**  If polling fails

#### `poll_analysis_grouping_job_batch_to_completion`

```python
def poll_analysis_grouping_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple analysis grouping jobs until all complete or timeout.

**Arguments:**
 - **job_ids:**  List of job IDs
 - **interval:**  Polling interval in seconds (default: 20)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> List of final job status details for all jobs

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If jobs time out
 - **IRPAPIError:**  If polling fails

#### `get_analysis_job`

```python
def get_analysis_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve analysis job status by job ID.

**Arguments:**
 - **job_id:**  Job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_analysis_job_to_completion`

```python
def poll_analysis_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll analysis job until completion or timeout.

**Arguments:**
 - **job_id:**  Job ID
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job times out
 - **IRPAPIError:**  If polling fails

#### `search_analysis_jobs`

```python
def search_analysis_jobs(
    self,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search analysis jobs with optional filtering.

**Arguments:**
 - **filter:**  Optional filter string (default: "")
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of analysis job dicts

**Raises:**
 - **IRPAPIError:**  If search fails

#### `poll_analysis_job_batch_to_completion`

```python
def poll_analysis_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple analysis jobs until all complete or timeout.

**Arguments:**
 - **job_ids:**  List of job IDs
 - **interval:**  Polling interval in seconds (default: 20)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> List of final job status details for all jobs

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If jobs time out
 - **IRPAPIError:**  If polling fails

#### `search_analyses`

```python
def search_analyses(
    self,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search analysis results with optional filtering.

**Arguments:**
 - **filter:**  Optional filter string (default: "")
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of analysis result dicts

**Raises:**
 - **IRPAPIError:**  If search fails

#### `search_analyses_paginated`

```python
def search_analyses_paginated(self, filter: str = '') -> List[Dict[str, Any]]
```

Search all analysis results with automatic pagination.

Fetches all pages of results matching the filter criteria.

**Arguments:**
 - **filter:**  Optional filter string (default: "")

**Returns:**
> Complete list of all matching analysis results across all pages

**Raises:**
 - **IRPAPIError:**  If search fails

#### `get_analysis_by_name`

```python
def get_analysis_by_name(self, analysis_name: str, edm_name: str) -> Dict[str, Any]
```

Get an analysis by name and EDM name.

**Arguments:**
 - **analysis_name:**  Name of the analysis
 - **edm_name:**  Name of the EDM (exposure database)

**Returns:**
> Dict containing analysis details

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If analysis not found or multiple matches

#### `delete_analysis`

```python
def delete_analysis(self, analysis_id: int) -> None
```

Delete an analysis by ID.

**Arguments:**
 - **analysis_id:**  Analysis ID to delete

**Raises:**
 - **IRPValidationError:**  If analysis_id is invalid
 - **IRPAPIError:**  If deletion fails

#### `get_analysis_by_app_analysis_id`

```python
def get_analysis_by_app_analysis_id(self, app_analysis_id: int) -> Dict[str, Any]
```

Retrieve analysis by appAnalysisId (the ID used in the application/UI).

**Arguments:**
 - **app_analysis_id:**  Application analysis ID (e.g., 35810)

**Returns:**
> Dict containing analysisId and exposureResourceId

**Raises:**
 - **IRPValidationError:**  If app_analysis_id is invalid
 - **IRPAPIError:**  If request fails or analysis not found

#### `get_elt`

```python
def get_elt(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]
```

Retrieve Event Loss Table (ELT) for an analysis.

**Arguments:**
 - **analysis_id:**  Analysis ID
 - **perspective_code:**  One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
 - **exposure_resource_id:**  Exposure resource ID (portfolio ID from analysis)
 - **filter:**  Optional filter string (e.g., "eventId IN (1, 2, 3)" or "eventId = 123")
 - **limit:**  Optional maximum number of records to return
 - **offset:**  Optional number of records to skip (for pagination)

**Returns:**
> List of ELT records containing eventId, positionValue, stdDevI, stdDevC, etc.

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails

#### `get_ep`

```python
def get_ep(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int
) -> List[Dict[str, Any]]
```

Retrieve EP (Exceedance Probability) metrics for an analysis.

**Arguments:**
 - **analysis_id:**  Analysis ID
 - **perspective_code:**  One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
 - **exposure_resource_id:**  Exposure resource ID (portfolio ID from analysis)

**Returns:**
> List of EP curve data (OEP, AEP, CEP, TCE curves)

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails

#### `get_stats`

```python
def get_stats(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int
) -> List[Dict[str, Any]]
```

Retrieve statistics for an analysis.

**Arguments:**
 - **analysis_id:**  Analysis ID
 - **perspective_code:**  One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
 - **exposure_resource_id:**  Exposure resource ID (portfolio ID from analysis)

**Returns:**
> List of statistical metrics

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails

#### `get_plt`

```python
def get_plt(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]
```

Retrieve Period Loss Table (PLT) for an analysis.

Note: PLT is only available for HD (High Definition) analyses.

**Arguments:**
 - **analysis_id:**  Analysis ID
 - **perspective_code:**  One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
 - **exposure_resource_id:**  Exposure resource ID (portfolio ID from analysis)
 - **filter:**  Optional filter string (e.g., "eventId IN (1, 2, 3)" or "eventId = 123")
 - **limit:**  Optional maximum number of records to return (default: 100000)
 - **offset:**  Optional number of records to skip (for pagination)

**Returns:**
> List of PLT records containing event dates, loss dates, and loss amounts

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails

#### `get_regions`

```python
def get_regions(self, analysis_id: int) -> List[Dict[str, Any]]
```

Retrieve region/peril breakdown for an analysis or group.

This is used to build the regionPerilSimulationSet for grouping requests.
Each region entry contains framework, peril, region codes, and simulation identifiers
(rateSchemeId for ELT, petId for PLT).

**Arguments:**
 - **analysis_id:**  Analysis or group ID

**Returns:**
> List of region dicts containing:
>     - region: Region code (e.g., "NA")
>     - subRegion: Sub-region code (e.g., "I2")
>     - peril: Peril code (e.g., "EQ", "WS")
>     - rateSchemeId: Event rate scheme ID (for ELT framework)
>     - framework: Framework type ("ELT" or "PLT")
>     - analysisId: The analysis ID
>     - modelProfileId: Model profile ID
>     - petId: PET ID (for PLT/HD framework)
>     - numSamples: Number of samples
>     - periods: Number of periods
>     - applyContractFlag: Contract application flag
>     - engineVersion: Engine version (e.g., "RL23", "HDv2.0")

**Raises:**
 - **IRPValidationError:**  If analysis_id is invalid
 - **IRPAPIError:**  If request fails

#### `submit_analysis_export_job`

```python
def submit_analysis_export_job(
    self,
    analysis_id: int,
    loss_details: List[Dict[str, Any]],
    file_extension: str = 'PARQUET'
) -> Tuple[int, Dict[str, Any]]
```

Submit an analysis results export job.

**Arguments:**
 - **analysis_id:**  ID of the analysis to export
 - **loss_details:**  List of loss detail configurations, each containing:
   - metricType: str (e.g., "LOSS_TABLES")
   - outputLevels: List[str] (e.g., ["Portfolio"])
   - perspectiveCodes: List[str] (e.g., ["GU", "GR"])
 - **file_extension:**  Export file format (default: "PARQUET")

**Returns:**
> Tuple of (job_id, request_body)

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If the analysis doesn't exist or request fails

---

## `irp_integration.rdm`

RDM (Risk Data Model) export operations.

Handles exporting analysis results to RDM via databridge.

### `class RDMManager`

Manager for RDM export operations.

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    analysis_manager: Optional[Any] = None,
    edm_manager: Optional[Any] = None
)
```

Initialize RDM manager.

**Arguments:**
 - **client:**  IRP API client instance
 - **analysis_manager:**  Optional AnalysisManager instance
 - **edm_manager:**  Optional EDMManager instance

#### `export_analyses_to_rdm`

```python
def export_analyses_to_rdm(
    self,
    server_name: str,
    rdm_name: str,
    analysis_names: List[str],
    skip_missing: bool = False
) -> Dict[str, Any]
```

Export multiple analyses to RDM (Risk Data Model) and poll to completion.

**Arguments:**
 - **server_name:**  Database server name
 - **rdm_name:**  Name for the RDM
 - **analysis_names:**  List of analysis names to export
 - **skip_missing:**  If True, skip missing analyses instead of raising an error

**Returns:**
> Dict containing final export job status

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If export fails or analyses not found

#### `submit_rdm_export_job`

```python
def submit_rdm_export_job(
    self,
    server_name: str,
    rdm_name: str,
    analysis_names: List[str],
    database_id: Optional[int] = None,
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> Dict[str, Any]
```

Submit RDM export job.

Performs validation (server lookup, RDM existence check, analysis URI
resolution) and submits the export job.

**Arguments:**
 - **server_name:**  Database server name
 - **rdm_name:**  Name for the RDM
 - **analysis_names:**  List of analysis and group names to export
 - **database_id:**  Optional database ID (for appending to existing RDM)
 - **analysis_edm_map:**  Optional mapping of analysis names to EDM names.
   Used to look up analyses by name + EDM (since analysis names are only
   unique within an EDM). If not provided, lookups use name only.
 - **group_names:**  Optional set of known group names. Items in this set are
   looked up as groups (by name only), all others are looked up as
   analyses (by name + EDM if mapping provided).
 - **skip_missing:**  If True (default), skip analyses/groups that don't exist
   instead of raising an error. If all items are missing, returns
   a result with job_id=None and skipped=True.

**Returns:**
> Dict containing:
>     - job_id: RDM export job ID (int), or None if skipped
>     - skipped: True if job was skipped (all items missing)
>     - skipped_items: List of item names that were not found and skipped
>     - included_items: List of item names that were found and included
>     - skip_reason: Reason for skipping (if skipped=True)

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If job submission fails, or if skip_missing=False and items not found

#### `get_rdm_export_job`

```python
def get_rdm_export_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve RDM export job status by job ID.

**Arguments:**
 - **job_id:**  Job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_rdm_export_job_to_completion`

```python
def poll_rdm_export_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll RDM export job until completion or timeout.

**Arguments:**
 - **job_id:**  Job ID
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job times out
 - **IRPAPIError:**  If polling fails

#### `get_rdm_database_id`

```python
def get_rdm_database_id(self, rdm_name: str, server_name: str = 'databridge-1') -> int
```

Get database ID for an existing RDM by name.

**Arguments:**
 - **rdm_name:**  Name of the RDM
 - **server_name:**  Name of the database server (default: "databridge-1")

**Returns:**
> Database ID

**Raises:**
 - **IRPAPIError:**  If RDM not found

#### `get_rdm_database_full_name`

```python
def get_rdm_database_full_name(self, rdm_name: str, server_name: str = 'databridge-1') -> str
```

Get full database name for an existing RDM by name prefix.

**Arguments:**
 - **rdm_name:**  Name prefix of the RDM
 - **server_name:**  Name of the database server (default: "databridge-1")

**Returns:**
> Full database name

**Raises:**
 - **IRPAPIError:**  If RDM not found

#### `search_databases`

```python
def search_databases(
    self,
    server_name: str,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search databases on a server.

**Arguments:**
 - **server_name:**  Name of the database server
 - **filter:**  Optional filter string (e.g., 'databaseName="MyRDM"')
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of database records

**Raises:**
 - **IRPAPIError:**  If request fails

#### `search_databases_paginated`

```python
def search_databases_paginated(self, server_name: str, filter: str = '') -> List[Dict[str, Any]]
```

Search all databases on a server with automatic pagination.

Fetches all pages of results matching the filter criteria.

**Arguments:**
 - **server_name:**  Name of the database server
 - **filter:**  Optional filter string (e.g., 'databaseName="MyRDM"')

**Returns:**
> Complete list of all matching database records across all pages

**Raises:**
 - **IRPAPIError:**  If request fails

#### `submit_delete_rdm_job`

```python
def submit_delete_rdm_job(self, rdm_name: str, server_name: str = 'databridge-1') -> str
```

Submit job to delete an RDM from the databridge server.

**Arguments:**
 - **rdm_name:**  Name prefix of the RDM to delete
 - **server_name:**  Name of the database server (default: "databridge-1")

**Returns:**
> Job ID for the delete operation

**Raises:**
 - **IRPAPIError:**  If RDM not found or delete request fails

#### `get_databridge_job`

```python
def get_databridge_job(self, job_id: str) -> str
```

Get the status of a databridge job.

**Arguments:**
 - **job_id:**  Job ID from databridge operation (e.g., delete RDM)

**Returns:**
> Job status string

**Raises:**
 - **IRPAPIError:**  If request fails

#### `poll_delete_rdm_job_to_completion`

```python
def poll_delete_rdm_job_to_completion(self, job_id: str, interval: int = 10, timeout: int = 600000) -> str
```

Poll delete RDM job until completion or timeout.

Valid statuses:
- "Enqueued": Job queued for processing
- "Processing": Job in progress
- "Succeeded": Job completed successfully
- Any other status is treated as an error

**Arguments:**
 - **job_id:**  Job ID from delete operation
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status string ("Succeeded")

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job fails or times out
 - **IRPAPIError:**  If polling fails

#### `add_group_access_to_rdm`

```python
def add_group_access_to_rdm(
    self,
    database_name: str,
    group_id: Optional[str] = None,
    server_name: str = 'databridge-1'
) -> Dict[str, Any]
```

Add group access to an RDM database.

**Arguments:**
 - **database_name:**  Name of the RDM database
 - **group_id:**  Group ID to grant access to. If None, uses DATABRIDGE_GROUP_ID
   environment variable.
 - **server_name:**  Name of the database server (default: "databridge-1")

**Returns:**
> Dict containing the API response

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPAPIError:**  If request fails or group_id is not configured

#### `search_imported_rdms`

```python
def search_imported_rdms(
    self,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search imported RDMs.

**Arguments:**
 - **filter:**  Optional filter string (e.g., 'name="MyRDM"')
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of imported RDM records

**Raises:**
 - **IRPAPIError:**  If request fails

#### `submit_rdm_import_job`

```python
def submit_rdm_import_job(
    self,
    rdm_name: str,
    edm_name: str,
    rdm_file_path: str
) -> Tuple[int, Dict[str, Any]]
```

Submit RDM import job with S3 file upload.

This method handles the complete RDM import workflow:
1. Search EDMs to get the resource URI
2. Create import folder (get S3 credentials)
3. Upload RDM .bak file to S3
4. Submit import job

**Arguments:**
 - **rdm_name:**  Name for the imported RDM
 - **edm_name:**  Name of the EDM to import into
 - **rdm_file_path:**  Path to the .bak file to import

**Returns:**
> Tuple of (job_id, request_body) where request_body is the HTTP request payload

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPFileError:**  If file upload fails
 - **IRPAPIError:**  If API calls fail

---

## `irp_integration.risk_data_job`

Risk data job management operations.

Handles job status tracking, polling, and batch polling for all platform risk data jobs via the unified /platform/riskdata/v1/jobs endpoint.

### `class RiskDataJobManager`

Manager for risk data job status tracking and polling.

#### `__init__`

```python
def __init__(self, client: irp_integration.client.Client)
```

Initialize risk data job manager.

**Arguments:**
 - **client:**  IRP API client instance

#### `get_risk_data_job`

```python
def get_risk_data_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve job status by job ID.

**Arguments:**
 - **job_id:**  Job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `search_risk_data_jobs`

```python
def search_risk_data_jobs(
    self,
    filter: str = '',
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

Search risk data jobs with optional filtering.

**Arguments:**
 - **filter:**  Optional filter string (default: "")
 - **limit:**  Maximum results per page (default: 100)
 - **offset:**  Offset for pagination (default: 0)

**Returns:**
> List of risk data job dicts

**Raises:**
 - **IRPAPIError:**  If search fails

#### `poll_risk_data_job_to_completion`

```python
def poll_risk_data_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll risk data job until completion or timeout.

Returns on any terminal status (FINISHED, FAILED, or CANCELLED) — the
caller must inspect the returned ``status`` (see the workflow contract
in ``client.py``).

**Arguments:**
 - **job_id:**  Job ID
 - **interval:**  Polling interval in seconds
 - **timeout:**  Maximum timeout in seconds

**Returns:**
> Dict containing the final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If the job times out
 - **IRPAPIError:**  If a status request fails

#### `poll_risk_data_job_batch_to_completion`

```python
def poll_risk_data_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple risk data jobs until all complete or timeout.

**Arguments:**
 - **job_ids:**  List of job IDs
 - **interval:**  Polling interval in seconds (default: 20)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> List of final job status details for all jobs

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If jobs time out
 - **IRPAPIError:**  If polling fails

---

## `irp_integration.import_job`

Import job management operations.

Provides a centralized interface for submitting, tracking, and polling platform import jobs. Uses the /platform/import/v1/jobs endpoint.

### `class ImportJobManager`

Manager for platform import job operations (EDM, RDM, MRI).

#### `__init__`

```python
def __init__(
    self,
    client: irp_integration.client.Client,
    edm_manager: Optional[Any] = None,
    rdm_manager: Optional[Any] = None,
    mri_manager: Optional[Any] = None
)
```

Initialize ImportJobManager.

**Arguments:**
 - **client:**  IRP API client instance
 - **edm_manager:**  Optional EDMManager instance for EDM import routing
 - **rdm_manager:**  Optional RDMManager instance for RDM import routing
 - **mri_manager:**  Optional MRIImportManager instance for MRI import routing

#### `submit_job`

```python
def submit_job(self, import_type: str, **kwargs) -> Tuple[int, Dict[str, Any]]
```

Submit an import job, routing to the appropriate manager based on type.

**Arguments:**
 - **import_type:**  Type of import - "EDM", "RDM", or "MRI"
 - ****kwargs:**  Arguments passed to the underlying submit method.

   For EDM (routed to EDMManager.submit_edm_import_job):
       edm_name (str): Name for the EDM
       edm_file_path (str): Path to the .bak file
       server_name (str): Database server name (default: "sql-instance-1")

   For RDM (routed to RDMManager.submit_rdm_import_job):
       rdm_name (str): Name for the RDM
       edm_name (str): Name of the target EDM
       rdm_file_path (str): Path to the .bak file

   For MRI (routed to MRIImportManager.submit_mri_import_job):
       edm_name (str): Target EDM name
       portfolio_name (str): Target portfolio name
       accounts_file_path (str): Path to accounts CSV file
       locations_file_path (str): Path to locations CSV file
       mapping_file_path (str, optional): Path to .mff mapping file
       delimiter (str): File delimiter (default: "TAB")

**Returns:**
> Tuple of (job_id, request_body)

**Raises:**
 - **IRPValidationError:**  If import_type is invalid or kwargs are wrong
 - **IRPAPIError:**  If submission fails

#### `get_import_job`

```python
def get_import_job(self, job_id: int) -> Dict[str, Any]
```

Get import job status by job ID.

**Arguments:**
 - **job_id:**  Import job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_import_job_to_completion`

```python
def poll_import_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll import job until completion or timeout.

**Arguments:**
 - **job_id:**  Import job ID
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job times out
 - **IRPAPIError:**  If polling fails

---

## `irp_integration.export_job`

Export job management operations.

Provides an interface for tracking and polling platform export jobs. Uses the /platform/export/v1/jobs endpoint.

### `class ExportJobManager`

Manager for platform export job operations.

#### `__init__`

```python
def __init__(self, client: irp_integration.client.Client)
```

Initialize ExportJobManager.

**Arguments:**
 - **client:**  IRP API client instance

#### `get_export_job`

```python
def get_export_job(self, job_id: int) -> Dict[str, Any]
```

Get export job status by job ID.

**Arguments:**
 - **job_id:**  Export job ID

**Returns:**
> Dict containing job status details

**Raises:**
 - **IRPValidationError:**  If job_id is invalid
 - **IRPAPIError:**  If request fails

#### `poll_export_job_to_completion`

```python
def poll_export_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll export job until completion or timeout.

**Arguments:**
 - **job_id:**  Export job ID
 - **interval:**  Polling interval in seconds (default: 10)
 - **timeout:**  Maximum timeout in seconds (default: 600000)

**Returns:**
> Final job status details

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job times out
 - **IRPAPIError:**  If polling fails

#### `download_export_results`

```python
def download_export_results(self, job_id: int, output_dir: str) -> str
```

Download exported analysis results for a completed export job.

Fetches the job, extracts the downloadUrl from the DOWNLOAD_RESULTS task,
and streams the zip file to the output directory.

**Arguments:**
 - **job_id:**  Export job ID (must be FINISHED)
 - **output_dir:**  Directory to save the downloaded file

**Returns:**
> Path to the downloaded file

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPJobError:**  If job is not finished
 - **IRPAPIError:**  If download URL not found or download fails

---

## `irp_integration.s3`

S3 upload and download operations for IRP Integration.

Handles file uploads to AWS S3 using temporary credentials provided by Moody's Risk Modeler API, and file downloads from CloudFront/presigned URLs.

### `class S3Manager`

Manager for S3 upload and CloudFront download operations.

#### `__init__`

```python
def __init__(
    self,
    transfer_config: Optional[boto3.s3.transfer.TransferConfig] = None
)
```

Initialize S3 Manager.

**Arguments:**
 - **transfer_config:**  Optional boto3 TransferConfig for multipart uploads.
   If not provided, uses default optimized settings.

#### `upload_file`

```python
def upload_file(
    self,
    file_path: str,
    upload_details: Dict[str, Any],
    content_type: Optional[str] = None
) -> None
```

Upload file to S3 using credentials from API response.

This method handles the S3 upload for EDM/RDM import workflows.
It extracts the upload URL and credentials from the API response
and performs a multipart upload.

**Arguments:**
 - **file_path:**  Path to the file to upload
 - **upload_details:**  Upload details dict from create import folder response,
   containing 'uploadUrl' and 'presignParams' (with base64-encoded
   credentials)
 - **content_type:**  Optional content type override. If not provided,
   inferred from file extension.

**Raises:**
 - **IRPValidationError:**  If parameters are invalid or required fields missing
 - **IRPFileError:**  If file upload fails

**Example:**
> ```python
> # From create import folder response:
> # response['uploadDetails']['exposureFile']
> upload_details = {
>     "fileUri": "platform/import/v1/folders/39073/files/105108",
>     "presignParams": {
>         "accessKeyId": "<base64>",
>         "secretAccessKey": "<base64>",
>         "sessionToken": "<base64>",
>         "path": "<base64>",
>         "region": "<base64>"
>     },
>     "uploadUrl": "https://bucket.s3.amazonaws.com/path/to/file.bak"
> }
> s3_manager.upload_file("/path/to/file.bak", upload_details)
> ```

#### `upload_fileobj`

```python
def upload_fileobj(
    self,
    fileobj: <class 'BinaryIO'>,
    upload_details: Dict[str, Any],
    content_type: str
) -> None
```

Upload file-like object to S3 using credentials from API response.

**Arguments:**
 - **fileobj:**  File-like object (e.g., BytesIO, open file in 'rb' mode)
 - **upload_details:**  Upload details dict from create import folder response
 - **content_type:**  Content type for the upload (required for streams)

**Raises:**
 - **IRPValidationError:**  If parameters are invalid or required fields missing
 - **IRPFileError:**  If file upload fails

#### `upload_file_from_credentials`

```python
def upload_file_from_credentials(
    self,
    file_path: str,
    credentials: Dict[str, str],
    bucket: str,
    key: str,
    content_type: Optional[str] = None
) -> None
```

Upload file to S3 using pre-decoded credentials.

Lower-level method for cases where credentials are already decoded
(e.g., MRI import workflow).

**Arguments:**
 - **file_path:**  Path to the file to upload
 - **credentials:**  Dict with decoded AWS credentials:
   - aws_access_key_id: str
   - aws_secret_access_key: str
   - aws_session_token: str
   - s3_region: str
 - **bucket:**  S3 bucket name
 - **key:**  S3 object key (path within bucket)
 - **content_type:**  Optional content type override

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPFileError:**  If file upload fails

#### `download_from_url`

```python
def download_from_url(
    self,
    url: str,
    destination_path: str,
    chunk_size: int = 8192,
    timeout: int = 300
) -> None
```

Download file from CloudFront or presigned URL to local path.

**Arguments:**
 - **url:**  Full URL including any signed parameters
 - **destination_path:**  Local path to save the file
 - **chunk_size:**  Download chunk size in bytes (default: 8192)
 - **timeout:**  Request timeout in seconds (default: 300)

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPFileError:**  If download fails or file cannot be written

#### `download_from_url_to_fileobj`

```python
def download_from_url_to_fileobj(
    self,
    url: str,
    fileobj: <class 'BinaryIO'>,
    chunk_size: int = 8192,
    timeout: int = 300
) -> None
```

Download file from CloudFront or presigned URL to file-like object.

**Arguments:**
 - **url:**  Full URL including any signed parameters
 - **fileobj:**  File-like object to write to (must be opened in binary write mode)
 - **chunk_size:**  Download chunk size in bytes (default: 8192)
 - **timeout:**  Request timeout in seconds (default: 300)

**Raises:**
 - **IRPValidationError:**  If parameters are invalid
 - **IRPFileError:**  If download fails or write fails

---

## `irp_integration.reference_data`

Reference data management operations.

Handles retrieval and creation of reference data including model profiles, output profiles, event rate schemes, currencies, and tags.

### `class ReferenceDataManager`

Manager for reference data operations.

#### `__init__`

```python
def __init__(self, client: irp_integration.client.Client)
```

Initialize reference data manager.

**Arguments:**
 - **client:**  IRP API client instance

#### `get_model_profiles`

```python
def get_model_profiles(self) -> Dict[str, Any]
```

Retrieve all model profiles.

**Returns:**
> Dict containing model profile list

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_model_profile_by_name`

```python
def get_model_profile_by_name(self, profile_name: str) -> Dict[str, Any]
```

Retrieve model profile by name.

**Arguments:**
 - **profile_name:**  Model profile name

**Returns:**
> Dict containing model profile details

**Raises:**
 - **IRPValidationError:**  If profile_name is invalid
 - **IRPAPIError:**  If request fails

#### `get_output_profiles`

```python
def get_output_profiles(self) -> List[Dict[str, Any]]
```

Retrieve all output profiles.

**Returns:**
> List of output profile dicts

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_output_profile_by_name`

```python
def get_output_profile_by_name(self, profile_name: str) -> List[Dict[str, Any]]
```

Retrieve output profile by name.

**Arguments:**
 - **profile_name:**  Output profile name

**Returns:**
> List of matching output profile dicts

**Raises:**
 - **IRPValidationError:**  If profile_name is invalid
 - **IRPAPIError:**  If request fails

#### `get_event_rate_schemes`

```python
def get_event_rate_schemes(self) -> Dict[str, Any]
```

Retrieve all active event rate schemes.

**Returns:**
> Dict containing event rate scheme list

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_event_rate_scheme_by_name`

```python
def get_event_rate_scheme_by_name(
    self,
    scheme_name: str,
    peril_code: Optional[str] = None,
    model_region_code: Optional[str] = None
) -> Dict[str, Any]
```

Retrieve event rate scheme by name with optional peril and region filtering.

When the same event rate scheme name exists for multiple peril/region combinations,
use the peril_code and model_region_code parameters to filter to the correct one.
These values can be obtained from the corresponding model profile.

**Arguments:**
 - **scheme_name:**  Event rate scheme name
 - **peril_code:**  Optional peril code (e.g., "CS", "WS") to filter results
 - **model_region_code:**  Optional model region code (e.g., "NACS", "NAWS") to filter results

**Returns:**
> Dict containing event rate scheme details

**Raises:**
 - **IRPValidationError:**  If scheme_name is invalid
 - **IRPAPIError:**  If request fails

#### `search_currencies`

```python
def search_currencies(self, where_clause: str = '') -> Dict[str, Any]
```

Search currencies with optional filtering.

**Arguments:**
 - **where_clause:**  Optional filter clause

**Returns:**
> Dict containing currencies (with an 'items' list)

**Raises:**
 - **IRPAPIError:**  If request fails

#### `search_currency_scheme_vintages`

```python
def search_currency_scheme_vintages(self, where_clause: str = '') -> Dict[str, Any]
```

Search currency scheme vintages with optional filtering.

**Arguments:**
 - **where_clause:**  Optional filter clause

**Returns:**
> Dict containing currency scheme vintages

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_latest_currency_scheme_vintage`

```python
def get_latest_currency_scheme_vintage(self) -> Dict[str, Any]
```

Get the latest RMS currency scheme vintage by effective date.

**Returns:**
> Dict containing the currency scheme vintage with the most recent effectiveDate

**Raises:**
 - **IRPAPIError:**  If request fails or no vintages found

#### `get_analysis_currency`

```python
def get_analysis_currency(self) -> Dict[str, str]
```

Get currency dict for analysis requests.

Attempts to get the latest RMS currency scheme vintage from the API.
Falls back to default values if the API call fails.

**Returns:**
> Currency dict with asOfDate, code, scheme, and vintage

#### `get_currency_by_name`

```python
def get_currency_by_name(self, currency_name: str) -> Dict[str, Any]
```

Retrieve currency by name.

**Arguments:**
 - **currency_name:**  Currency name

**Returns:**
> Dict containing currency details

**Raises:**
 - **IRPValidationError:**  If currency_name is invalid
 - **IRPAPIError:**  If request fails

#### `get_tag_by_name`

```python
def get_tag_by_name(self, tag_name: str) -> List[Dict[str, Any]]
```

Retrieve tag by name.

**Arguments:**
 - **tag_name:**  Tag name

**Returns:**
> List of dicts containing tag details

**Raises:**
 - **IRPValidationError:**  If tag_name is invalid
 - **IRPAPIError:**  If request fails

#### `create_tag`

```python
def create_tag(self, tag_name: str) -> Dict[str, str]
```

Create new tag.

**Arguments:**
 - **tag_name:**  Tag name

**Returns:**
> Dict with tag ID

**Raises:**
 - **IRPValidationError:**  If tag_name is invalid
 - **IRPAPIError:**  If request fails

#### `get_tag_ids_from_tag_names`

```python
def get_tag_ids_from_tag_names(self, tag_names: List[str]) -> List[int]
```

Get or create tags by names and return their IDs.

This method will create tags if they don't already exist.

**Arguments:**
 - **tag_names:**  List of tag names

**Returns:**
> List of tag IDs

**Raises:**
 - **IRPValidationError:**  If tag_names is empty
 - **IRPAPIError:**  If request fails

#### `get_all_simulation_sets`

```python
def get_all_simulation_sets(self) -> List[Dict[str, Any]]
```

Get all active simulation sets.

Simulation sets map event rate scheme IDs to simulation set IDs
for ELT-based analyses. This fetches all active sets which can be
filtered locally by event rate scheme ID.

**Returns:**
> List of simulation set dicts

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_simulation_set_by_event_rate_scheme_id`

```python
def get_simulation_set_by_event_rate_scheme_id(self, event_rate_scheme_id: int) -> Dict[str, Any]
```

Get simulation set by event rate scheme ID.

For ELT analyses, the simulationSetId in grouping requests comes from
this lookup using the eventRateSchemeId from the analysis regions.

**Arguments:**
 - **event_rate_scheme_id:**  Event rate scheme ID from analysis regions

**Returns:**
> Dict containing simulation set details with 'id' being the simulationSetId

**Raises:**
 - **IRPAPIError:**  If request fails or simulation set not found

#### `get_simulation_set_by_region_peril_and_engine`

```python
def get_simulation_set_by_region_peril_and_engine(
    self,
    region_code: str,
    peril_code: str,
    engine_version: str
) -> Dict[str, Any]
```

Get simulation set by regionCode, perilCode, and engineVersion.

This is a fallback method used when eventRateSchemeId is not available.
The lookup uses regionCode + perilCode to build the broader modelRegionCode
(e.g., "NA" + "WS" = "NAWS") since SimulationSet entries use broader regional
codes, not sub-region-specific codes like "HTWS".

Note: When multiple simulation sets match, returns the one with highest id
(most recent). For precise matching, use get_simulation_set_by_event_rate_scheme_id
with the eventRateSchemeId from the analysis additionalProperties.

**Arguments:**
 - **region_code:**  Region code (e.g., "NA", "US", "CB")
 - **peril_code:**  Peril code (e.g., "WS", "EQ", "FL")
 - **engine_version:**  Engine version (e.g., "RL23", "HDv2.0")

**Returns:**
> Dict containing simulation set details with 'id' being the simulationSetId

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If request fails or simulation set not found

#### `get_all_pet_metadata`

```python
def get_all_pet_metadata(self) -> List[Dict[str, Any]]
```

Get all PET (Probabilistic Event Table) metadata.

PET metadata maps PET IDs to simulation set IDs for PLT/HD-based analyses.

**Returns:**
> List of PET metadata dicts

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_pet_metadata_by_id`

```python
def get_pet_metadata_by_id(self, pet_id: int) -> Dict[str, Any]
```

Get PET metadata by PET ID.

For PLT/HD analyses, the simulationSetId in grouping requests is the
PET ID itself (the 'id' field from PET metadata).

**Arguments:**
 - **pet_id:**  PET ID from analysis regions

**Returns:**
> Dict containing PET metadata details

**Raises:**
 - **IRPValidationError:**  If pet_id is invalid
 - **IRPAPIError:**  If request fails or PET not found

#### `get_all_software_model_version_map`

```python
def get_all_software_model_version_map(self) -> List[Dict[str, Any]]
```

Get all active software model version mappings.

This maps engine versions to model versions for grouping requests.

**Returns:**
> List of version map dicts

**Raises:**
 - **IRPAPIError:**  If request fails

#### `get_model_version_by_engine_version`

```python
def get_model_version_by_engine_version(self, engine_version: str) -> str
```

Get model version for a given engine version.

Note: This method looks for any entry matching the softwareVersionCode.
For more precise matching, use get_model_version_by_engine_and_region.

**Arguments:**
 - **engine_version:**  Engine version string (e.g., "HDv2.0", "RL23")

**Returns:**
> Model version string (e.g., "2.0", "23.0")

**Raises:**
 - **IRPValidationError:**  If engine_version is invalid
 - **IRPAPIError:**  If request fails or mapping not found

#### `get_model_version_by_engine_region_peril`

```python
def get_model_version_by_engine_region_peril(self, engine_version: str, region_code: str, peril_code: str) -> str
```

Get model version for a given engine version, region code, and peril code.

This provides a precise lookup using the broader modelRegionCode (e.g., "NAWS")
built from regionCode + perilCode, since SoftwareModelVersionMap uses broader
codes, not sub-region-specific codes like "HTWS".

**Arguments:**
 - **engine_version:**  Engine version string (e.g., "HDv2.0", "RL23")
 - **region_code:**  Region code (e.g., "NA", "US", "CB")
 - **peril_code:**  Peril code (e.g., "WS", "EQ", "FL")

**Returns:**
> Model version string (e.g., "2.0", "11.0")

**Raises:**
 - **IRPValidationError:**  If inputs are invalid
 - **IRPAPIError:**  If request fails or mapping not found

---

## `irp_integration.databridge`

Data Bridge (SQL Server) operations.

Provides SQL Server connectivity via pyodbc with named connections, parameterized query execution using {{ param }} template syntax, and file-based SQL execution. Designed for interacting with Moody's Data Bridge databases.

**Connection Management:**

Supports multiple named MSSQL connections configured via environment variables. Each connection requires:

MSSQL_{CONNECTION_NAME}_SERVER   - Server hostname or IP (required) MSSQL_{CONNECTION_NAME}_USER     - SQL auth username (required) MSSQL_{CONNECTION_NAME}_PASSWORD - SQL auth password (required) MSSQL_{CONNECTION_NAME}_PORT     - Port (optional, defaults to 1433)

Global settings: MSSQL_DRIVER     - ODBC driver name (default: 'ODBC Driver 18 for SQL Server') MSSQL_TRUST_CERT - Trust server certificate (default: 'yes') MSSQL_TIMEOUT    - Connection timeout in seconds (default: '30')

**Parameter Substitution:**

SQL queries support named parameters using {{ param_name }} syntax. Parameters are context-aware: identifiers (inside brackets or as part of table names) are substituted raw, while values are escaped with SQL injection protection.

### `class ExpressionTemplate`

*Bases:* `string.Template`

Custom Template class for SQL parameter substitution.

Uses {{ PARAM }} syntax with space padding to avoid conflicts with SQL syntax. Example: SELECT * FROM table WHERE id = {{ ID }}

### `class DataBridgeManager`

Manager for SQL Server (Data Bridge) operations.

Unlike other managers, DataBridgeManager does not depend on the HTTP Client. It connects directly to SQL Server via pyodbc. It can be used standalone or attached to IRPClient as client.databridge.

**Args:**

default_connection: Default connection name used when no connection

is specified in method calls. Defaults to 'DATABRIDGE'.

**Environment Variables (per connection):**

MSSQL_{CONNECTION_NAME}_SERVER   - Server hostname or IP (required) MSSQL_{CONNECTION_NAME}_USER     - SQL auth username (required) MSSQL_{CONNECTION_NAME}_PASSWORD - SQL auth password (required) MSSQL_{CONNECTION_NAME}_PORT     - Port (optional, defaults to 1433)

**Global Environment Variables:**

MSSQL_DRIVER     - ODBC driver name (default: 'ODBC Driver 18 for SQL Server') MSSQL_TRUST_CERT - Trust server certificate (default: 'yes') MSSQL_TIMEOUT    - Connection timeout in seconds (default: '30')

**Example:**

# Via IRPClient from irp_integration import IRPClient client = IRPClient() df = client.databridge.execute_query(

"SELECT * FROM portfolios WHERE value > {{ min_value }}", params={'min_value': 1000000}, connection='DATABRIDGE', database='DataWarehouse'

)

# Standalone from irp_integration.databridge import DataBridgeManager db = DataBridgeManager(default_connection='DATABRIDGE') df = db.execute_query("SELECT 1 AS test", database='master')

#### `__init__`

```python
def __init__(self, default_connection: str = 'DATABRIDGE')
```

Initialize the Data Bridge manager.

**Arguments:**
 - **default_connection:**  Name of the connection used when a query does
   not specify one (default: "DATABRIDGE")

#### `get_connection_config`

```python
def get_connection_config(self, connection_name: Optional[str] = None) -> Dict[str, str]
```

Get connection configuration for a named MSSQL connection.

Reads configuration from environment variables following the pattern
MSSQL_{CONNECTION_NAME}_{SETTING}.

**Arguments:**
 - **connection_name:**  Name of the connection (e.g., 'DATABRIDGE', 'ANALYTICS').
   Defaults to the manager's default_connection.

**Returns:**
> Dictionary with connection parameters: server, port, driver,
> trust_cert, timeout, user, password.

**Raises:**
 - **IRPValidationError:**  If required environment variables are missing.

**Example:**
> config = db.get_connection_config('DATABRIDGE')
> # Returns: {'server': 'db.company.com', 'user': 'svc', ...}

#### `build_connection_string`

```python
def build_connection_string(
    self,
    connection_name: Optional[str] = None,
    database: Optional[str] = None
) -> str
```

Build ODBC connection string for SQL Server.

**Arguments:**
 - **connection_name:**  Name of the connection. Defaults to the
   manager's default_connection.
 - **database:**  Optional database name to connect to.

**Returns:**
> ODBC connection string.

**Example:**
> conn_str = db.build_connection_string('DATABRIDGE', database='MyDB')

#### `get_connection`

```python
def get_connection(
    self,
    connection_name: Optional[str] = None,
    database: Optional[str] = None
)
```

Context manager for SQL Server database connections.

Automatically handles connection lifecycle: opens connection,
yields it for use, and closes on exit (even if exception occurs).

**Arguments:**
 - **connection_name:**  Name of the connection to use. Defaults to
   the manager's default_connection.
 - **database:**  Optional database name to connect to.

**Yields:**
> pyodbc.Connection object.

**Raises:**
 - **IRPDataBridgeConnectionError:**  If connection fails.

**Example:**
> with db.get_connection('DATABRIDGE', database='MyDB') as conn:
>     cursor = conn.cursor()
>     cursor.execute("SELECT * FROM portfolios")
>     rows = cursor.fetchall()

#### `test_connection`

```python
def test_connection(self, connection_name: Optional[str] = None) -> bool
```

Test if a SQL Server connection is working.

**Arguments:**
 - **connection_name:**  Name of the connection to test. Defaults to
   the manager's default_connection.

**Returns:**
> True if connection successful, False otherwise.

**Example:**
> if db.test_connection('DATABRIDGE'):
>     print("Connection successful!")

#### `execute_query`

```python
def execute_query(
    self,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> pandas.DataFrame
```

Execute SELECT query and return results as DataFrame.

**Arguments:**
 - **query:**  SQL SELECT query (supports {{ param_name }} placeholders).
 - **params:**  Query parameters as dictionary.
 - **connection:**  Name of the SQL Server connection to use.
   Defaults to the manager's default_connection.
 - **database:**  Optional database name to connect to.

**Returns:**
> pandas DataFrame with query results.

**Raises:**
 - **IRPDataBridgeQueryError:**  If query execution fails.

**Example:**
> df = db.execute_query(
>     "SELECT * FROM portfolios WHERE value > {{ min_value }}",
>     params={'min_value': 1000000},
>     connection='DATABRIDGE',
>     database='DataWarehouse'
> )

#### `execute_scalar`

```python
def execute_scalar(
    self,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> Any
```

Execute query and return single scalar value (first column of first row).

**Arguments:**
 - **query:**  SQL query returning single value.
 - **params:**  Query parameters.
 - **connection:**  Name of the SQL Server connection to use.
   Defaults to the manager's default_connection.
 - **database:**  Optional database name to connect to.

**Returns:**
> Single value from query result (or None if no results).

**Raises:**
 - **IRPDataBridgeQueryError:**  If query execution fails.

**Example:**
> count = db.execute_scalar(
>     "SELECT COUNT(*) FROM portfolios WHERE value > {{ min_value }}",
>     params={'min_value': 1000000},
>     connection='DATABRIDGE',
>     database='DataWarehouse'
> )

#### `execute_command`

```python
def execute_command(
    self,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> int
```

Execute non-query command (INSERT, UPDATE, DELETE) and return rows affected.

**Arguments:**
 - **query:**  SQL command.
 - **params:**  Query parameters.
 - **connection:**  Name of the SQL Server connection to use.
   Defaults to the manager's default_connection.
 - **database:**  Optional database name to connect to.

**Returns:**
> Number of rows affected.

**Raises:**
 - **IRPDataBridgeQueryError:**  If command execution fails.

**Example:**
> rows = db.execute_command(
>     "UPDATE portfolios SET status = {{ status }} WHERE value < {{ min_value }}",
>     params={'status': 'INACTIVE', 'min_value': 100000},
>     connection='DATABRIDGE',
>     database='DataWarehouse'
> )
> print(f"Updated {rows} rows")

#### `execute_query_from_file`

```python
def execute_query_from_file(
    self,
    file_path: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> List[pandas.DataFrame]
```

Execute SQL query from file and return results as list of DataFrames.

Handles both single-statement queries and multi-statement scripts
(e.g., scripts with USE statements followed by SELECT). Each result
set is returned as a separate DataFrame in the list.

**Arguments:**
 - **file_path:**  Path to SQL file (absolute or relative to cwd).
 - **params:**  Query parameters (supports {{ param_name }} placeholders).
 - **connection:**  Name of the SQL Server connection to use.
   Defaults to the manager's default_connection.
 - **database:**  Optional database name to connect to.

**Returns:**
> List of pandas DataFrames, one per result set.

**Raises:**
 - **IRPValidationError:**  If SQL file does not exist.
 - **IRPDataBridgeQueryError:**  If query execution fails.

**Example:**
> results = db.execute_query_from_file(
>     'C:/sql/extract_policies.sql',
>     params={'cycle_name': 'Q1-2025', 'run_date': '2025-01-15'},
>     connection='DATABRIDGE',
>     database='AnalyticsDB'
> )
> df = results[0]  # First result set

---

## `irp_integration.exceptions`

Custom exception classes for IRP Integration module.

These exceptions provide clear, structured error handling for different failure scenarios when interacting with Moody's Risk Modeler API.

### `class IRPIntegrationError`

*Bases:* `builtins.Exception`

Base exception for all IRP integration errors.

### `class IRPAPIError`

*Bases:* `IRPIntegrationError`

API request or response errors.

Raised when HTTP requests fail, responses are malformed, or API returns unexpected status codes.

### `class IRPAuthenticationError`

*Bases:* `IRPIntegrationError`

Bearer-token authentication errors.

Raised when bearer-token login or token refresh fails (bad credentials, missing access token in the response, etc.).

### `class IRPValidationError`

*Bases:* `IRPIntegrationError`

Input validation errors.

Raised when method parameters fail validation checks (e.g., empty strings, invalid IDs, missing files).

### `class IRPWorkflowError`

*Bases:* `IRPIntegrationError`

Workflow execution errors.

Raised when workflows fail to complete successfully, timeout, or return error status.

### `class IRPReferenceDataError`

*Bases:* `IRPIntegrationError`

Reference data lookup errors.

Raised when required reference data (treaty types, currencies, etc.) cannot be found or retrieved.

### `class IRPFileError`

*Bases:* `IRPIntegrationError`

File operation errors.

Raised when file operations fail (file not found, invalid format, upload errors, etc.).

### `class IRPJobError`

*Bases:* `IRPIntegrationError`

Job management errors.

Raised when job submission, status retrieval, or result fetching encounters issues.

### `class IRPDataBridgeError`

*Bases:* `IRPIntegrationError`

Data Bridge (SQL Server) operation errors.

Base exception for all SQL Server / Data Bridge failures including connection, configuration, and query errors.

### `class IRPDataBridgeConnectionError`

*Bases:* `IRPDataBridgeError`

Data Bridge connection errors.

Raised when SQL Server connection fails (bad credentials, unreachable server, driver not installed).

### `class IRPDataBridgeQueryError`

*Bases:* `IRPDataBridgeError`

Data Bridge query execution errors.

Raised when SQL query execution fails, parameter substitution fails, or SQL file cannot be read.

---

## `irp_integration.validators`

Input validation utilities for IRP Integration module.

Provides reusable validation functions that raise descriptive IRPValidationError exceptions when validation fails.

### Functions

#### `validate_non_empty_string`

```python
def validate_non_empty_string(value: Any, param_name: str) -> None
```

Validate that a value is a non-empty string.

**Arguments:**
 - **value:**  Value to validate
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If value is not a non-empty string

#### `validate_positive_int`

```python
def validate_positive_int(value: Any, param_name: str) -> None
```

Validate that a value is a positive integer.

**Arguments:**
 - **value:**  Value to validate
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If value is not a positive integer

#### `validate_non_negative_int`

```python
def validate_non_negative_int(value: Any, param_name: str) -> None
```

Validate that a value is a non-negative integer.

**Arguments:**
 - **value:**  Value to validate
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If value is not a non-negative integer

#### `validate_file_exists`

```python
def validate_file_exists(file_path: str, param_name: str = 'file_path') -> None
```

Validate that a file exists at the given path.

**Arguments:**
 - **file_path:**  Path to file
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If file does not exist

#### `validate_list_not_empty`

```python
def validate_list_not_empty(value: Any, param_name: str) -> None
```

Validate that a value is a non-empty list.

**Arguments:**
 - **value:**  Value to validate
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If value is not a non-empty list

#### `validate_positive_float`

```python
def validate_positive_float(value: Any, param_name: str) -> None
```

Validate that a value is a positive float.

**Arguments:**
 - **value:**  Value to validate
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If value is not a positive float

#### `validate_non_negative_float`

```python
def validate_non_negative_float(value: Any, param_name: str) -> None
```

Validate that a value is a non-negative float.

**Arguments:**
 - **value:**  Value to validate
 - **param_name:**  Parameter name for error message

**Raises:**
 - **IRPValidationError:**  If value is not a non-negative float

---

## `irp_integration.utils`

Utility functions for IRP Integration module.

Provides common helper functions for response parsing, data extraction, and reference data lookup operations.

### Functions

#### `get_location_header`

```python
def get_location_header(
    response: requests.models.Response,
    error_context: str = 'response'
) -> str
```

Get Location header from response.

**Arguments:**
 - **response:**  HTTP response object
 - **error_context:**  Context description for error message

**Returns:**
> Location header value

**Raises:**
 - **IRPAPIError:**  If the Location header is missing

#### `extract_id_from_location_header`

```python
def extract_id_from_location_header(
    response: requests.models.Response,
    error_context: str = 'response'
) -> str
```

Extract ID from Location header in HTTP response.

**Arguments:**
 - **response:**  HTTP response object
 - **error_context:**  Context description for error message

**Returns:**
> Extracted ID string

**Raises:**
 - **IRPAPIError:**  If Location header is missing

#### `decode_base64_field`

```python
def decode_base64_field(encoded_value: str, field_name: str) -> str
```

Decode a base64-encoded field value.

**Arguments:**
 - **encoded_value:**  Base64-encoded string
 - **field_name:**  Field name for error message

**Returns:**
> Decoded string

**Raises:**
 - **IRPAPIError:**  If decoding fails

#### `decode_presign_params`

```python
def decode_presign_params(presign_params: Dict[str, Any]) -> Dict[str, str]
```

Decode base64 credentials from MRI import file credentials response.

**Arguments:**
 - **presign_params:**  Response JSON containing encoded credentials

**Returns:**
> Dict with decoded credential fields

**Raises:**
 - **IRPAPIError:**  If required fields missing or decoding fails

#### `extract_analysis_id_from_workflow_response`

```python
def extract_analysis_id_from_workflow_response(workflow: Dict[str, Any]) -> Optional[str]
```

Extract analysis ID from workflow response.

**Arguments:**
 - **workflow:**  Workflow response dict

**Returns:**
> Analysis ID if found, None otherwise

**Raises:**
 - **IRPAPIError:**  If required fields are missing from workflow response

---

## `irp_integration.constants`

API endpoint constants and status/code maps for the Risk Modeler API.

**Defines:**

- Endpoint path templates, grouped by area. Most contain ``str.format`` placeholders (e.g. ``{exposureId}``, ``{jobId}``) that callers fill in with resource IDs before issuing the request.
- Workflow status groupings: ``WORKFLOW_COMPLETED_STATUSES`` (terminal) and ``WORKFLOW_IN_PROGRESS_STATUSES`` (non-terminal). See ``client.py`` for how these drive polling and the terminal-status contract.
- Code maps that translate human-readable names to the short API codes: ``TREATY_TYPES``, ``TREATY_ATTACHMENT_BASES``, ``TREATY_ATTACHMENT_LEVELS``, and ``PERSPECTIVE_CODES``.

---
