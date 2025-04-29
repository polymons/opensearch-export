# opensearch-export
Simple script that queries OpenSearch logs and exports them to CSV or JSON.

# To run the script:

### Create virtual environment
```bash
python -m venv .venv
```

### Activate virtual environment
```bash
.venv\Scripts\activate
```

### Install dependencies
```bash
python -r pip install ./requirements.txt
or
pip install -r requirements.txt
```

# How it Works
The script connects to an OpenSearch cluster using the credentials and connection details provided in `parameters.json`. It then executes a query based on the configuration in the same file, fetching data within a specified time range and matching defined criteria. The results are streamed and saved to either a JSON or CSV file, as configured.

# Configuration (`parameters.json`)
The `parameters.json` file contains all the necessary settings for the script to run. Here's a breakdown of the main sections:

*   **`connection`**: Specifies the OpenSearch host, port, username, password, and SSL settings.
*   **`index`**: The index pattern to query (e.g., `your-index-pattern-*`).
*   **`timespan`**: Defines the start and end time for the data query in `YYYY-MM-DDTHH:mm:ss` format.
*   **`query`**: Contains the specific query details (see below).
*   **`output`**: Configures the output format (`json` or `csv`), file path, and batch size for fetching data.
*   **`scroll`**: Sets the scroll time for fetching large datasets.

## Defining a Query
The `query` object within `parameters.json` allows you to specify the search criteria using the OpenSearch Query DSL.

*   **`_source`**: (Optional) A list of fields to include in the results. If omitted, all fields are returned.
*   **`bool_conditions`**: (Optional) Defines boolean clauses (`must`, `should`, `must_not`, `filter`) to combine multiple query criteria. You can nest boolean queries and use various query types like `term`, `match`, `range`, `wildcard`, `exists`, etc.

**Example Query Structure:**

```json
"query": {
  "_source": [
    "timestamp",
    "applicationName",
    "fields.eventCode"
  ],
  "bool_conditions": {
    "must": [
      {
        "bool": {
          "should": [
            {
              "bool": {
                "must": [
                  {"wildcard": {"applicationName": "app-prefix*"}},
                  {"term": {"fields.eventCode.keyword": "EVENT_CODE_1"}}
                ]
              }
            },
            {
              "bool": {
                "must": [
                  {"wildcard": {"applicationName": "another-app-prefix*"}},
                  {"exists": {"field": "fields.correlationId"}}
                ]
              }
            }
          ],
          "minimum_should_match": 1
        }
      }
    ]
  }
}
```

This example fetches specific fields (`_source`) for documents where the `applicationName` starts with `app-prefix*` AND has `EVENT_CODE_1`, OR where the `applicationName` starts with `another-app-prefix*` AND the `fields.correlationId` exists.

# Running the Script

Once configured, run the script from your activated virtual environment:

```bash
python fetchData.py
```

You can optionally provide a path to a different configuration file:

```bash
python fetchData.py /path/to/your/custom_parameters.json
```

