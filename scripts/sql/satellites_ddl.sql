CREATE TABLE sat_store (
    store_hashkey VARCHAR(32) NOT NULL,
    country VARCHAR(64),
    city VARCHAR(128),
    state VARCHAR(64),
    postal_code VARCHAR(16),
    region VARCHAR(64),
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL,
    PRIMARY KEY (store_hashkey, load_dts)
) DISTRIBUTED BY (store_hashkey);

CREATE TABLE sat_product (
    product_hashkey VARCHAR(32) NOT NULL,
    category VARCHAR(64),
    sub_category VARCHAR(128),
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL,
    PRIMARY KEY (product_hashkey, load_dts)
) DISTRIBUTED BY (product_hashkey);

CREATE TABLE sat_sale (
    sale_hashkey VARCHAR(32) NOT NULL,
    sales NUMERIC,
    quantity NUMERIC,
    discount NUMERIC,
    profit NUMERIC,
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL,
    PRIMARY KEY (sale_hashkey, load_dts)
) DISTRIBUTED BY (sale_hashkey);

CREATE TABLE sat_shipmode (
    shipmode_hashkey VARCHAR(32) NOT NULL,
    description VARCHAR(256),
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL,
    PRIMARY KEY (shipmode_hashkey, load_dts)
) DISTRIBUTED BY (shipmode_hashkey);

CREATE TABLE sat_segment (
    segment_hashkey VARCHAR(32) NOT NULL,
    segment_desc VARCHAR(256),
    load_dts TIMESTAMP NOT NULL,
    record_source VARCHAR(64) NOT NULL,
    PRIMARY KEY (segment_hashkey, load_dts)
) DISTRIBUTED BY (segment_hashkey);
