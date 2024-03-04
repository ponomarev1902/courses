DROP TABLE IF EXISTS temp_lesson;
CREATE TABLE temp_lesson
(
    id INTEGER PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    homework_url VARCHAR(500),
    teacher_id INTEGER,
    stream_module_id INTEGER,
    deleted_at TIMESTAMP(0),
    online_lesson_join_url VARCHAR(255),
    online_lesson_recording_url VARCHAR(255)
);