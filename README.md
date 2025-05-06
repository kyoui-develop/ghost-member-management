```mermaid
graph LR
    source["Ghost Admin API"] --> |fetch| pipeline["Pipeline"]
    pipeline --> |update| source
    bq["BigQuery"] --> |fetch| pipeline
    pipeline --> |synchronize| bq