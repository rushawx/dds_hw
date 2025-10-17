CREATE TABLE hub_store (
    store_hashkey VARCHAR(32) PRIMARY KEY,
    store_business_key VARCHAR(128) NOT NULL,
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL
) DISTRIBUTED BY (store_hashkey);

CREATE TABLE hub_product (
    product_hashkey VARCHAR(32) PRIMARY KEY,
    product_business_key VARCHAR(128) NOT NULL,
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL
) DISTRIBUTED BY (product_hashkey);

CREATE TABLE hub_sale (
    sale_hashkey VARCHAR(32) PRIMARY KEY,
    sale_business_key VARCHAR(128) NOT NULL,
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL
) DISTRIBUTED BY (sale_hashkey);

CREATE TABLE hub_shipmode (
    shipmode_hashkey VARCHAR(32) PRIMARY KEY,
    shipmode_business_key VARCHAR(64) NOT NULL,
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL
) DISTRIBUTED BY (shipmode_hashkey);

CREATE TABLE hub_segment (
    segment_hashkey VARCHAR(32) PRIMARY KEY,
    segment_business_key VARCHAR(64) NOT NULL,
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL
) DISTRIBUTED BY (segment_hashkey);
