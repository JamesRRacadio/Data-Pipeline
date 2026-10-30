# Data-Pipeline
Data pipeline for data engineering project.

```mermaid
graph LR
    A[producer.py <br><i>Fictional FI Data</i>] -->|Streaming JSON| B(Docker Container <br><b>Apache Kafka</b>)
    B -->|Streaming Data| C[consumer.py]
    C -->|Daily Batch <br><i>12:00 PM UTC / 100 Recs</i>| D[<b>AWS S3 Bucket</b>]

    style B fill:#333,stroke:#fff,stroke-width:2px,color:#fff
    style D fill:#FF9900,stroke:#333,stroke-width:1px,color:#fff
