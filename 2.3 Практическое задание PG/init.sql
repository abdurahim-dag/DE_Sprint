BEGIN;

create database "tested";

create schema "tested";

create table tested.employees
(
    "EMP_ID" bigserial  primary key,
    "EMP_FULLNAME"      varchar(200) not null,
    "EMP_BIRTHDAY"      date,
    "EMP_JOINED"        date,
    "EMP_POST"          varchar(200),
    "EMP_LEVEL"         varchar(200),
    "EMP_SALARY_LEVEL"  numeric(10, 2),
    "EMP_DEPARTMENT_ID" bigint       not null
        references tested.departments,
    "EMP_DRIVER"        boolean
);

CREATE TABLE IF NOT EXISTS tested."departments"
(
    "DEP_ID" bigserial NOT NULL,
    "DEP_NAME" character varying(200),
    "DEP_LEAD" character varying(200),
    "DEP_EMP_COUNT" smallint,
    PRIMARY KEY ("DEP_ID")
);

CREATE TABLE IF NOT EXISTS tested."SCORE"
(
    "SCORE_ID" bigserial NOT NULL,
    "EMP_ID" bigint,
    "SCORE_VALUE" "char",
    "SCORE_QUARTER" smallint,
    PRIMARY KEY ("SCORE_ID"),
	CONSTRAINT check_allowed CHECK ("SCORE_VALUE" IN ('A', 'B', 'C', 'D', 'E'))
);

ALTER TABLE IF EXISTS tested.employees
    ADD FOREIGN KEY ("EMP_DEPARTMENT_ID")
    REFERENCES tested."departments" ("DEP_ID") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS tested."SCORE"
    ADD FOREIGN KEY ("EMP_ID")
    REFERENCES tested.employees ("EMP_ID") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;