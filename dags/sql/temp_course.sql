DROP TABLE IF EXISTS course_temp;
CREATE TABLE course_temp
(
    id INTEGER PRIMARY KEY,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP(0),
    icon_url VARCHAR(255),
    is_auto_course_enroll BOOLEAN,
    is_demo_enroll BOOLEAN
)