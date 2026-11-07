# 12 – REST API Metadata, Key Vault Authentication and Pagination (Chapter 13)

## 1. Confirm the API metadata

The repository contains `api_metadata_v2 (2).json`. It defines four data endpoints:

```json
[
  {
    "endpoint_name": "payments",
    "relative_url": "payments",
    "target_folder": "api/payments"
  },
  {
    "endpoint_name": "deliveries",
    "relative_url": "deliveries",
    "target_folder": "api/deliveries"
  },
  {
    "endpoint_name": "ratings",
    "relative_url": "ratings",
    "target_folder": "api/ratings"
  },
  {
    "endpoint_name": "order-status",
    "relative_url": "order-status",
    "target_folder": "api/order_status"
  }
]
```

The transcript also discusses a watermark/control endpoint used to support incremental API processing. It is separate from these four business-data endpoints.

## 2. Understand the API URL construction

The instructor deploys a mock REST API (on Render) and uses:
- Base URL: the host root of the deployed API.
- Relative URL: one of the values from the metadata file.
- Target folder: where the response is landed in ADLS.

For example, the payments request is conceptually:

```text
<API_BASE_URL>/payments
```

Use the current API base URL from the API deployment/repository. Do not copy a temporary or expired demo host without testing it.

## 3. Test API outside ADF first

Before creating the ADF linked service:
1. Open the API documentation or use the provided API code.
2. Test the `payments` endpoint.
3. Confirm a response is returned.
4. Repeat for `deliveries`, `ratings`, and `order-status`.
5. Confirm the response is JSON and identify the pagination response fields.
6. Identify the `updated_at`/updated-after field used for incremental filtering.

The video enables authorization, pagination and an updated-after mechanism in its mock API.

## 4. Store API token in Azure Key Vault

1. Create/open Azure Key Vault.
2. Add the API authorization token as a secret.
3. Enable the ADF managed identity.
4. Grant that identity permission to read the secret using the vault's configured RBAC/access-policy model.
5. In ADF, create a Key Vault linked service.
6. Test it.
7. Reference the secret from the REST linked service or secure pipeline configuration.

The transcript explicitly says the API token is stored in Key Vault so it is not exposed in pipeline code.

## 5. Create REST linked service

1. ADF Studio → Manage → Linked services → New.
2. Select REST.
3. Base URL: enter the API base URL.
4. Configure the authentication method to match the API implementation (the demo uses a token-based authorization header).
5. Retrieve the token securely from Key Vault.
6. Set required request headers. For a bearer-token API, the header is commonly:

```text
Authorization: Bearer <secret-value>
```

Use the exact scheme expected by the API code; do not assume Bearer if the API uses another scheme.
7. Test connection and save.

## 6. Create REST dataset and metadata Lookup

1. Create a REST dataset using the REST linked service.
2. Parameterize the relative URL if using one dataset for all endpoints.
3. Create a JSON metadata dataset pointing to `api_metadata_v2 (2).json` in ADLS.
4. Add Lookup to read the JSON metadata.
5. Ensure it returns all four objects.
6. Add ForEach with Items:

```text
@activity('Lookup_API_Metadata').output.value
```

## 7. Configure Copy activity inside ForEach

Source:
- REST dataset.
- Relative URL parameter = `@item().relative_url`.
- Method and query parameters must match the API contract.
- Authorization header is supplied through the secure linked-service/Key Vault configuration.

Sink:
- ADLS Gen2 dataset.
- Target folder parameter = `@item().target_folder`.
- Land the response as raw JSON under `bronze/api/...`.

The metadata fields are intentionally simple: endpoint name, relative URL and target folder. The endpoint name can also be passed to the downstream Databricks dispatcher.

## 8. Configure pagination

The mock API in the transcript supports pagination so large result sets are returned in chunks.

1. Inspect a real response and identify how the API signals the next page (page number, offset, continuation token or next URL).
2. In the REST Copy source settings, configure the pagination rules to match the actual response.
3. Test first, middle and final pages.
4. Confirm all expected records are landed.
5. Avoid excessive parallel requests if the API imposes rate limits.

Do not invent pagination JSON paths; they must match the deployed API's response contract.

## 9. API incremental watermark concept

The transcript describes an `updated_after`/updated timestamp mechanism and a separate watermark endpoint/JSON. The business endpoints expose update timestamps; the watermark metadata tracks the last processed boundary for each endpoint.

A correct incremental sequence is:
1. Read the endpoint's last successful watermark.
2. Call the endpoint with the API's supported updated-after filter.
3. Land the changed records.
4. Validate the response and row count.
5. Advance the endpoint watermark only after success.

The transcript does not provide a complete exact ADF expression or final watermark JSON schema in the spoken content. Use the API code and metadata/control files from the repository for those exact fields.

## 10. Validate

- [ ] Four business endpoint metadata rows loaded.
- [ ] Key Vault secret read by ADF.
- [ ] REST linked service test succeeds.
- [ ] Each relative URL returns JSON.
- [ ] Pagination returns every page.
- [ ] Output folders match metadata.
- [ ] Watermark/update-after behavior tested separately.
