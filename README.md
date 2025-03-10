```mermaid
graph LR
    source["Ghost Admin API"] <--> pipeline["Pipeline"]
    pipeline <--> storage["BigQuery"]