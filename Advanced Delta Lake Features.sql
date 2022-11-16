-- Databricks notebook source
CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

describe extended students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC var=dbutils.fs.ls("dbfs:/user/hive/warehouse/")
-- MAGIC display(var)

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC var=dbutils.fs.ls("dbfs:/user/hive/warehouse/students")
-- MAGIC display(var)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC var=dbutils.fs.ls("dbfs:/user/hive/warehouse/students/_delta_log")
-- MAGIC display(var)

-- COMMAND ----------

describe detail students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`dbfs:/user/hive/warehouse/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

select * from students

-- COMMAND ----------

optimize students zorder by id

-- COMMAND ----------

DESCRIBE history students

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 6

-- COMMAND ----------

select * from students VERSION AS OF 8

-- COMMAND ----------

INSERT INTO students
VALUES 
  (40, "New_Ted", 40.7),
  (50, "New_Tiffany", 50.5),
  (60, "New_Vini", 60.3);
  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC var=dbutils.fs.ls("dbfs:/user/hive/warehouse/students")
-- MAGIC display(var)

-- COMMAND ----------

optimize students zorder by id

-- COMMAND ----------

describe history students

-- COMMAND ----------

select * from students

-- COMMAND ----------

select * from students version as of 10

-- COMMAND ----------

delete from students

-- COMMAND ----------

select * from students --version as of 10

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 10

-- COMMAND ----------

describe history students



-- COMMAND ----------

-- MAGIC %python
-- MAGIC var=dbutils.fs.ls("dbfs:/user/hive/warehouse/students")
-- MAGIC display(var)

-- COMMAND ----------

VACUUM students RETAIN 150 HOURS

-- COMMAND ----------


