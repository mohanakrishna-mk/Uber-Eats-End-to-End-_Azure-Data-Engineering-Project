# 05 – REST API Ingestion with Azure Data Factory

## 1. Inspect the API contract first

Use the repository's `REST_API_Migration_Step_by_Step.docx`, API metadata JSON file and `routing/` artifacts. Confirm:
- Base URL and resource paths.
- HTTP method.
- Authentication type (API key, Basic, OAuth2, managed identity, etc.).
- Required headers and query parameters.
- Pagination mechanism (page number, offset/limit, next link, continuation token).
- Rate limits and retry policy.
- Response JSON structure and expected fields.

Do not guess an endpoint or put a real API token in a committed JSON file.

## 2. Create a Key Vault secret (recommended)

1. Create/open Azure Key Vault.
2. Add the API secret using the API provider's supported credential.
3. Grant the ADF managed identity permission to read the secret (RBAC or access policy according to the vault configuration).
4. Create an ADF Key Vault linked service.
5. Reference the secret from the REST linked service or pipeline securely.

For OAuth2, configure token acquisition/refresh according to the API's actual auth flow. Do not assume a static token will remain valid.

## 3. Create the REST linked service

1. ADF Studio → Manage → Linked services → New.
2. Search for **REST**.
3. Set the base URL.
4. Select the correct authentication type.
5. Configure Key Vault references or secure parameters.
6. Test connection if the connector/API supports it.
7. Save.

## 4. Create the REST dataset

1. Author → New dataset → REST.
2. Select the REST linked service.
3. Set the relative URL/resource path if required.
4. Define request parameters and format based on the API.
5. Save.

The repository contains `api_metadata_v2 (2).json`; inspect its property names and resource definitions before wiring the dataset and pipeline.

## 5. Build a basic one-page copy

1. Create a pipeline.
2. Add Copy activity.
3. Source: REST dataset.
4. Configure relative URL, method and query parameters.
5. Sink: ADLS Gen2 JSON dataset or the raw format selected by the project.
6. Debug.
7. Inspect the raw response in ADLS.

Start with a small response and prove authentication and serialization before implementing pagination.

## 6. Configure pagination

Common API pagination patterns include:
- Page number: `page=1`, `page=2`, etc.
- Offset/limit: `offset=0&limit=100`.
- Continuation token: use the token returned by the previous response.
- Next URL: follow a response field containing the next link.

ADF's REST connector supports pagination rules for supported response patterns. Configure the rule to match the API's real response structure. Do not assume the field is called `next`; inspect the payload.

Test:
- First page.
- Middle page.
- Last page.
- Empty result.
- Expired token.
- Rate limit response (e.g. HTTP 429).

## 7. Metadata-driven API ingestion

If the API metadata JSON contains multiple entities/endpoints, use Lookup to read the metadata and ForEach to process each active endpoint. Pass the current endpoint/resource path and required query configuration to the REST dataset or Copy activity.

Use secure pipeline parameters for secrets. Keep non-secret endpoint metadata in version control if appropriate.

## 8. Reliability settings

- Configure retry count and retry interval for transient failures.
- Respect API rate limits.
- Use a reasonable request timeout.
- Avoid unbounded parallel calls.
- Capture endpoint, page/token, run ID, status, response status and error details in audit logs.
- Make ingestion restartable. Persist pagination state only if the API and design require it.
- Land raw responses before complex transformations so the source payload can be replayed.

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| 401 | Token validity, audience/scope, auth scheme |
| 403 | API permissions, subscription/key restrictions |
| 404 | Base URL and relative URL composition |
| 429 | Rate limit; reduce concurrency and implement backoff |
| Only first page copied | Pagination rule and response path |
| Empty output | Query params, date filters, response envelope |
| JSON parse failure | Response content type and actual payload |
| TLS/connectivity issue | DNS, firewall, certificate chain, outbound access |

## Completion checklist

- [ ] API contract and metadata reviewed.
- [ ] Authentication secret stored securely.
- [ ] REST linked service configured.
- [ ] Single-page copy succeeds.
- [ ] Pagination returns all expected pages.
- [ ] Retry/rate-limit behavior tested.
- [ ] Raw payloads land in the correct ADLS folder.
