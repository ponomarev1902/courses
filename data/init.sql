DROP TABLE IF EXISTS stream;
CREATE TABLE stream
(
id integer primary key,
course_id integer,
start_at timestamp,
end_at timestamp,
created_at timestamp,
updated_at timestamp,
deleted_at timestamp,
is_open boolean,
name varchar(255),
homework_deadline_days integer
);

INSERT INTO 
	stream
VALUES 
	 (1, 1, '2023-09-01', '2023-12-25', '2023-06-05', '2023-06-05', NULL, TRUE, 'stream A', 7)
	,(2, 1, '2023-09-01', '2023-12-25', '2023-06-05', '2023-06-05', NULL, TRUE, 'stream B', 9)
	,(3, 2, '2023-09-01', '2023-12-25', '2023-06-05', '2023-06-05', '2023-06-05', TRUE, 'stream C', 10)
	;

DROP TABLE IF EXISTS course;
CREATE TABLE course
(
id integer primary key,
title varchar(255),
created_at timestamp,
updated_at timestamp,
deleted_at timestamp(0),
icon_url varchar(255),
is_auto_course_enroll boolean,
is_demo_enroll boolean
);

INSERT INTO 
	course
VALUES 
	 (1, 'Computer Science', '2023-09-01', '2023-12-25', NULL, 'images.com/path/to/image/cs.png', TRUE, TRUE)
	,(2, 'Math', '2023-09-01', '2023-12-25', NULL, 'images.com/path/to/image/math.png', TRUE, TRUE)
	,(3, 'Machine Learning', '2023-09-01', '2023-12-25', NULL, 'images.com/path/to/image/ml.jpeg', FALSE, TRUE)
	;

DROP TABLE IF EXISTS stream_module;
CREATE TABLE stream_module
(
	id integer primary key,
	stream_id integer,
	title varchar(255),
	created_at timestamp,
	updated_at timestamp,
	order_in_stream integer,
	deleted_at timestamp
);

INSERT INTO 
	stream_module
VALUES
	(1, 1, 'module_A', '2023-01-05', '2023-01-05', 1, NULL)
	,(2, 1, 'module_B', '2023-01-05', '2023-01-05', 2, NULL)
	,(3, 2, 'module_C', '2023-01-05', '2023-01-05', 3, NULL)
	,(4, 2, 'module_D', '2023-01-05', '2023-01-05', 3, NULL)
	,(5, 3, 'module_E', '2023-01-05', '2023-01-05', 3, '2024-01-01')
	;

DROP TABLE IF EXISTS stream_module_lesson;
CREATE TABLE stream_module_lesson
(
id integer primary key,
title varchar(255),
description text,
start_at timestamp,
end_at timestamp,
homework_url varchar(500),
teacher_id integer,
stream_module_id integer,
deleted_at timestamp(0),
online_lesson_join_url varchar(255),
online_lesson_recording_url varchar(255)
);
INSERT INTO
	stream_module_lesson
VALUES
	(1, 'Lesson_1', 'Description of lesson 1', '2023-02-01', '2023-07-01', 'www.homework.ru/path/to/hw/ls1', 1, 1, NULL, 'www.online.com/path/to/lesson_1', 'ftp://path/to/record/1')
	,(2, 'Lesson_2', 'Description of lesson 2', '2023-02-01', '2023-07-01', 'www.homework.ru/path/to/hw/ls2', 1, 1, NULL, 'www.online.com/path/to/lesson_2', 'ftp://path/to/record/2')
	,(3, 'Lesson_3', 'Description of lesson 3', '2023-02-01', '2023-07-01', 'www.homework.ru/path/to/hw/ls3', 2, 2, NULL, 'www.online.com/path/to/lesson_c', 'ftp://path/to/record/3')
	,(4, 'Lesson_4', 'Description of lesson 4', '2023-02-01', '2023-07-01', 'www.homework.ru/path/to/hw/ls4', 1, 3, NULL, 'www.online.com/path/to/lesson_d', 'ftp://path/to/record/4')
	;