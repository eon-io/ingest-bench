CREATE TABLE kafka_source (
  `id` BIGINT NOT NULL,
  `partition_key` STRING NOT NULL,
  `event_time` TIMESTAMP(3) NOT NULL,
  `event_id` STRING NOT NULL,
  `event_type` STRING NOT NULL,
  `user_id` STRING NOT NULL,
  `source_id` STRING NOT NULL,
  `page_id` STRING NOT NULL,
  `account_id` STRING NOT NULL,
  `campaign_id` STRING NOT NULL,
  `asset_id` STRING NOT NULL,
  `country` STRING NOT NULL,
  `device_type` STRING NOT NULL,
  `os` STRING NOT NULL,
  `browser` STRING NOT NULL,
  `viewport` STRING NOT NULL,
  `price` DOUBLE NOT NULL,
  `cost` DOUBLE NOT NULL,
  `amount_micros` BIGINT NOT NULL,
  `latency_ms` BIGINT NOT NULL,
  `is_visible` BOOLEAN NOT NULL,
  `client_hash` STRING NOT NULL,
  `payload` BYTES NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = '@TOPIC@',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '@RUN_ID@',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro',
  'avro.timestamp_mapping.legacy' = 'false'
);

CREATE CATALOG ice WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.access-key-id' = 'admin',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  'client.region' = 'us-east-1',
  's3.secret-access-key' = 'password'
);

INSERT INTO ice.`@NAMESPACE@`.`@TABLE@` /*+ OPTIONS('distribution-mode' = 'hash', 'write-parallelism' = '8') */
SELECT `id`, `partition_key`, `event_time`, `event_id`, `event_type`, `user_id`, `source_id`, `page_id`, `account_id`, `campaign_id`, `asset_id`, `country`, `device_type`, `os`, `browser`, `viewport`, `price`, `cost`, `amount_micros`, `latency_ms`, `is_visible`, `client_hash`, `payload` FROM kafka_source;
