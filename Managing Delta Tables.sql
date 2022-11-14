-- Databricks notebook source
create database db_practice

-- COMMAND ----------

CREATE TABLE if not exists db_practice.students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

INSERT INTO db_practice.students VALUES (1, "Yve", 1.0);
INSERT INTO db_practice.students VALUES (2, "Omar", 2.5);
INSERT INTO db_practice.students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

select * from db_practice.students

-- COMMAND ----------

INSERT INTO db_practice.students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

UPDATE db_practice.students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

MERGE INTO db_practice.students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *
