-- нужно добавить ограничения
--create table dds.states
--(id serial, cop_state varchar(2), state_name varchar(20));
--ALTER TABLE dds.states ADD CONSTRAINT state_name_uindex UNIQUE (state_name);
--create table dds.directions
--(id serial, direction_id integer, direction_code varchar(10), direction_name varchar(100));
--ALTER TABLE dds.directions ADD CONSTRAINT direction_code_uindex UNIQUE (direction_code);
--create table dds.levels
--(id serial, training_period varchar(5), level_name varchar(20));
--ALTER TABLE dds.levels ADD CONSTRAINT level_name_uindex UNIQUE (level_name);
--create table dds.editors
--(id integer, username varchar(50), first_name varchar(50), last_name varchar(50), email varchar(50), isu_number varchar(6));
--ALTER TABLE dds.editors ADD CONSTRAINT editors_uindex UNIQUE (id);
--create table dds.units
--(id integer, unit_title varchar(100), faculty_id integer);
--ALTER TABLE dds.units ADD CONSTRAINT units_uindex UNIQUE (id);
--create table dds.up
--(id integer, plan_type varchar(8), direction_id integer, ns_id integer, edu_program_id integer, edu_program_name text, unit_id integer, level_id integer, university_partner text, up_country text, lang text, military_department boolean, selection_year integer);
--create table dds.wp
--(wp_id integer, discipline_code integer, wp_title text, wp_status integer, unit_id integer, wp_description text);
--ALTER TABLE dds.wp
--ALTER COLUMN discipline_code TYPE text;
--create table dds.wp_editor
--(wp_id integer, editor_id integer);
--create table dds.wp_up
--(wp_id integer, up_id integer);
--create table dds.wp_markup
--(id integer, title text, discipline_code integer, prerequisites text, outcomes text, prerequisites_cnt smallint, outcomes_cnt smallint);
--ALTER TABLE dds.wp_markup ADD CONSTRAINT wp_id_uindex UNIQUE (id);
--create table dds.online_courses
--(id integer, title text, institution varchar (100), discipline_code integer);


create table dds.states
(id serial, cop_state varchar(2), state_name varchar(20));
ALTER TABLE dds.states ADD CONSTRAINT state_name_uindex UNIQUE (state_name);
create table dds.editors
(id integer, username varchar(50), first_name varchar(50), last_name varchar(50), email varchar(50), isu_number varchar(6));
ALTER TABLE dds.editors ADD CONSTRAINT editors_uindex UNIQUE (id);
create table dds.up
(app_isu_id integer, on_check varchar(20), laboriousness integer, year integer, qualification varchar(20), update_ts timestamp, faculty varchar(50));
create table dds.wp
(wp_id integer, discipline_code integer, wp_title text, wp_status integer, unit_id integer, wp_description text, update_ts timestamp);
ALTER TABLE dds.wp
ALTER COLUMN discipline_code TYPE text;
create table dds.wp_editor
(wp_id integer, editor_id integer, update_ts timestamp);
create table dds.units
(id integer, unit_title varchar(100), faculty_id integer);
ALTER TABLE dds.units ADD CONSTRAINT units_uindex UNIQUE (id);

-- Добавляем первичный ключ для id в таблицу states
ALTER TABLE dds.states
ADD CONSTRAINT states_pk PRIMARY KEY (id);
ADD CONSTRAINT state_name_uindex UNIQUE (state_name);

-- Добавляем первичный ключ для id в таблицу editors
ALTER TABLE dds.editors
ADD CONSTRAINT editors_pk PRIMARY KEY (id);
ADD CONSTRAINT editors_uindex UNIQUE (editor_id);

-- Добавляем первичный ключ для app_isu_id в таблицу up
ALTER TABLE dds.up
ADD CONSTRAINT up_pk PRIMARY KEY (app_isu_id);

-- Добавляем первичный ключ для wp_id в таблицу wp
ALTER TABLE dds.wp
ADD CONSTRAINT wp_pk PRIMARY KEY (wp_id);

-- Добавляем составной первичный ключ для wp_id и editor_id в таблицу wp_editor
ALTER TABLE dds.wp_editor
ADD CONSTRAINT wp_editor_pk PRIMARY KEY (wp_id, editor_id);

-- Добавляем первичный ключ для id в таблицу units
ALTER TABLE dds.units
ADD CONSTRAINT units_pk PRIMARY KEY (id);


-- Добавляем внешний ключ для wp_status в таблицу states
ALTER TABLE dds.wp
ADD CONSTRAINT fk_wp_status
FOREIGN KEY (wp_status) REFERENCES dds.states(id);

-- Добавляем внешний ключ для unit_id в таблицу units
ALTER TABLE dds.wp
ADD CONSTRAINT fk_wp_unit_id
FOREIGN KEY (unit_id) REFERENCES dds.units(id);

-- Добавляем внешний ключ для editor_id и wp_id в таблице wp_editor
ALTER TABLE dds.wp_editor
ADD CONSTRAINT fk_wp_editor_editor
FOREIGN KEY (editor_id) REFERENCES dds.editors(id);
ALTER TABLE dds.wp_editor
ADD CONSTRAINT fk_wp_editor_wp_update
FOREIGN KEY (wp_id, update_ts) REFERENCES dds.wp (wp_id, update_ts);

-- Добавляем внешний ключ для wp_unit в таблицу wp
ALTER TABLE dds.wp
ADD CONSTRAINT fk_wp_unit
FOREIGN KEY (unit_id) REFERENCES dds.units(id);

alter table dds.up add column unit text
