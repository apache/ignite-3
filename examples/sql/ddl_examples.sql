-- Comprehensive Apache Ignite 3 DDL Syntax Demonstration
-- This file demonstrates all DDL commands supported by Apache Ignite 3

-- ============================================================================
-- CLEAN UP EXISTING OBJECTS (in reverse dependency order)
-- ============================================================================
DROP INDEX IF EXISTS idx_lang_country_code;
DROP INDEX IF EXISTS idx_country_code;
DROP INDEX IF EXISTS department_name_idx;
DROP INDEX IF EXISTS name_surname_idx;
DROP INDEX IF EXISTS department_city_idx;

DROP TABLE IF EXISTS country_language;
DROP TABLE IF EXISTS city;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS person;

DROP SCHEMA IF EXISTS test_schema CASCADE;

DROP ZONE IF EXISTS storage_zone;
DROP ZONE IF EXISTS example_zone;
DROP ZONE IF EXISTS my_example_zone;
DROP ZONE IF EXISTS renamed_zone;
DROP ZONE IF EXISTS my_zone;
DROP ZONE IF EXISTS scaled_zone;

-- ============================================================================
-- CREATE ZONE EXAMPLES
-- ============================================================================

-- Basic zone creation with case-insensitive identifier
CREATE ZONE IF NOT EXISTS example_zone STORAGE PROFILES['default'];

-- Zone with case-sensitive string name
CREATE ZONE IF NOT EXISTS "my_example_zone" STORAGE PROFILES['default'];

-- Zone with auto scale up configuration
CREATE ZONE IF NOT EXISTS storage_zone (PARTITIONS 50, REPLICAS 3, AUTO SCALE UP 300) STORAGE PROFILES ['default'];

-- Zone with auto scale down configuration
CREATE ZONE IF NOT EXISTS scaled_zone (AUTO SCALE DOWN 600) STORAGE PROFILES['default'];

-- Zone with node filtering for 'default' storage
-- This example requires at least one node that has the storage parameter set to 'default'
--CREATE ZONE IF NOT EXISTS my_zone (NODES FILTER '$[?(@.storage == "default")]') STORAGE PROFILES['default'];

-- ============================================================================
-- ALTER ZONE EXAMPLES
-- ============================================================================

-- Rename zone
ALTER ZONE IF EXISTS example_zone RENAME TO renamed_zone;

-- Set zone replicas
ALTER ZONE IF EXISTS renamed_zone SET REPLICAS 10;

-- Set multiple zone parameters
ALTER ZONE IF EXISTS renamed_zone SET (REPLICAS 5, QUORUM SIZE 3);

-- ============================================================================
-- CREATE SCHEMA EXAMPLES
-- ============================================================================

-- Basic schema creation
CREATE SCHEMA IF NOT EXISTS test_schema;

-- ============================================================================
-- CREATE TABLE EXAMPLES
-- ============================================================================

-- Basic table creation
CREATE TABLE IF NOT EXISTS person (
  id INT PRIMARY KEY,
  city_id INT,
  name VARCHAR,
  age INT,
  company VARCHAR
);

-- Table with distribution zone
CREATE TABLE IF NOT EXISTS country (
  code VARCHAR PRIMARY KEY,
  name VARCHAR,
  continent VARCHAR,
  region VARCHAR,
  surface_area DECIMAL(10,2),
  indep_year SMALLINT,
  population INT,
  life_expectancy DECIMAL(3,1),
  gnp DECIMAL(10,2),
  gnp_old DECIMAL(10,2),
  local_name VARCHAR,
  government_form VARCHAR,
  head_of_state VARCHAR,
  capital INT,
  code2 VARCHAR
) ZONE storage_zone STORAGE PROFILE 'default';

-- Table with composite primary key and zone
CREATE TABLE IF NOT EXISTS city (
  id INT,
  name VARCHAR,
  country_code VARCHAR,
  district VARCHAR,
  population INT,
  PRIMARY KEY (id, country_code)
) ZONE storage_zone STORAGE PROFILE 'default';

-- Table with composite primary key
CREATE TABLE IF NOT EXISTS country_language (
  country_code VARCHAR,
  language VARCHAR,
  is_official VARCHAR,
  percentage DECIMAL(4,1),
  PRIMARY KEY (country_code, language)
) ZONE storage_zone STORAGE PROFILE 'default';

-- Table with default values
-- (Recreate person table with default values)
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (
  id INT PRIMARY KEY,
  city_id INT DEFAULT 1,
  name VARCHAR,
  age INT,
  company VARCHAR
);

-- Table with UUID generation and defaults
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (
  id INT PRIMARY KEY,
  city_id INT DEFAULT 1,
  name VARCHAR(10),
  age INT,
  company VARCHAR,
  ttl TIMESTAMP WITH LOCAL TIME ZONE
);

-- ============================================================================
-- CREATE INDEX EXAMPLES
-- ============================================================================

-- Basic index
CREATE INDEX IF NOT EXISTS idx_country_code ON city (country_code);

-- Index with DESC and ASC ordering
CREATE INDEX IF NOT EXISTS department_name_idx ON person (city_id DESC, name ASC);

-- Hash index
CREATE INDEX IF NOT EXISTS name_surname_idx ON person USING HASH (name, age);

-- Sorted index
CREATE INDEX IF NOT EXISTS department_city_idx ON person USING SORTED (age ASC, city_id DESC);

-- Index on country_language table
CREATE INDEX IF NOT EXISTS idx_lang_country_code ON country_language (country_code);

-- ============================================================================
-- ALTER TABLE EXAMPLES
-- ============================================================================

-- Add column with IF EXISTS check
ALTER TABLE IF EXISTS person ADD number BIGINT;

-- Add multiple columns
ALTER TABLE person ADD COLUMN (code VARCHAR, gdp DOUBLE);

-- Drop single column
ALTER TABLE person DROP COLUMN company;

-- Drop column with IF EXISTS check
ALTER TABLE IF EXISTS person DROP COLUMN number;

-- Drop multiple columns
ALTER TABLE person DROP COLUMN (code, gdp);

-- Alter column data type
ALTER TABLE person ALTER COLUMN age SET DATA TYPE BIGINT;

-- Drop NOT NULL constraint
ALTER TABLE person ALTER COLUMN name DROP NOT NULL;

-- Set default value for column
ALTER TABLE person ALTER COLUMN city_id SET DEFAULT 1;

-- Drop default value
ALTER TABLE person ALTER COLUMN city_id DROP DEFAULT;

-- ============================================================================
-- DROP EXAMPLES
-- ============================================================================

-- Drop index
DROP INDEX IF EXISTS department_name_idx;

-- Drop table
DROP TABLE IF EXISTS person;

-- Drop schema with RESTRICT (default behavior)
DROP SCHEMA IF EXISTS test_schema RESTRICT;

-- Drop schema with CASCADE (drops all objects in schema)
-- DROP SCHEMA IF EXISTS test_schema CASCADE;

-- Drop zone
DROP ZONE IF EXISTS renamed_zone;

-- ============================================================================
-- SAMPLE DATA INSERTION (for demonstration)
-- ============================================================================

INSERT INTO country (code, name, continent, region, surface_area, indep_year, population, life_expectancy, gnp, gnp_old, local_name, government_form, head_of_state, capital, code2)
VALUES
('USA', 'United States', 'North America', 'North America', 9629091.00, 1776, 331000000, 78.9, 21427700.00, 20544343.00, 'United States', 'Federal Republic', 'President', 1, 'US'),
('CAN', 'Canada', 'North America', 'North America', 9984670.00, 1867, 38000000, 82.0, 1736425.00, 1653043.00, 'Canada', 'Constitutional Monarchy', 'Prime Minister', 2, 'CA');

INSERT INTO city (id, name, country_code, district, population)
VALUES
(1, 'Washington', 'USA', 'District of Columbia', 705749),
(2, 'Ottawa', 'CAN', 'Ontario', 994837),
(3, 'New York', 'USA', 'New York', 8175133),
(4, 'Toronto', 'CAN', 'Ontario', 2731571);

INSERT INTO country_language (country_code, language, is_official, percentage)
VALUES
('USA', 'English', 'T', 82.1),
('USA', 'Spanish', 'F', 10.7),
('CAN', 'English', 'T', 58.0),
('CAN', 'French', 'T', 22.0);