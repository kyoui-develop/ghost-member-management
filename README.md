```mermaid
graph LR
    source["Ghost Admin API"] <--> pipeline["Pipeline"]
    pipeline <--> bq["BigQuery"]