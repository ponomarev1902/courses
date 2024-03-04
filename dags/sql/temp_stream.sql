DROP TABLE IF EXISTS temp_stream;
CREATE TABLE temp_stream
(
    id INTEGER PRIMARY KEY,
    course_id INTEGER,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    is_open BOOLEAN,
    name VARCHAR(255),
    homework_deadline_days INTEGER
);