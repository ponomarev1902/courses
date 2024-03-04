DROP TABLE IF EXISTS temp_module;
CREATE TABLE temp_module
(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_in_stream INTEGER,
    deleted_at TIMESTAMP
);