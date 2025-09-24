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

DROP TABLE IF EXISTS countryLanguage;
DROP TABLE IF EXISTS city;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS person;

DROP SCHEMA IF EXISTS testSchema CASCADE;

DROP ZONE IF EXISTS storageZone;
DROP ZONE IF EXISTS exampleZone;
DROP ZONE IF EXISTS myExampleZone;
DROP ZONE IF EXISTS renamedZone;
DROP ZONE IF EXISTS myZone;
DROP ZONE IF EXISTS scaledZone;

-- ============================================================================
-- CREATE ZONE EXAMPLES
-- ============================================================================

-- Basic zone creation with case-insensitive identifier
CREATE ZONE IF NOT EXISTS exampleZone STORAGE PROFILES['default'];

-- Zone with case-sensitive string name
CREATE ZONE IF NOT EXISTS "myExampleZone" STORAGE PROFILES['default'];

-- Zone with auto scale up configuration
CREATE ZONE IF NOT EXISTS storageZone (PARTITIONS 50, REPLICAS 3, AUTO SCALE UP 300) STORAGE PROFILES ['default'];

-- Zone with auto scale down configuration
CREATE ZONE IF NOT EXISTS scaledZone (AUTO SCALE DOWN 600) STORAGE PROFILES['default'];

-- Zone with node filtering for SSD storage
-- This example requires at least one node that has the storage parameter set to SSD
--CREATE ZONE IF NOT EXISTS myZone (NODES FILTER '$[?(@.storage == "SSD")]') STORAGE PROFILES['default'];

-- ============================================================================
-- ALTER ZONE EXAMPLES
-- ============================================================================

-- Rename zone
ALTER ZONE IF EXISTS exampleZone RENAME TO renamedZone;

-- Set zone replicas
ALTER ZONE IF EXISTS renamedZone SET REPLICAS 10;

-- Set multiple zone parameters
ALTER ZONE IF EXISTS renamedZone SET (REPLICAS 5, QUORUM SIZE 3);

-- ============================================================================
-- CREATE SCHEMA EXAMPLES
-- ============================================================================

-- Basic schema creation
CREATE SCHEMA IF NOT EXISTS testSchema;

-- ============================================================================
-- CREATE TABLE EXAMPLES
-- ============================================================================

-- Basic table creation
CREATE TABLE IF NOT EXISTS person (
  Id INT PRIMARY KEY,
  City_id INT,
  Name VARCHAR,
  Age INT,
  Company VARCHAR
);

-- Table with distribution zone
CREATE TABLE IF NOT EXISTS country (
  Code VARCHAR PRIMARY KEY,
  Name VARCHAR,
  Continent VARCHAR,
  Region VARCHAR,
  Surface_area DECIMAL(10,2),
  Indep_year SMALLINT,
  Population INT,
  Life_expectancy DECIMAL(3,1),
  Gnp DECIMAL(10,2),
  Gnp_old DECIMAL(10,2),
  Local_name VARCHAR,
  Government_form VARCHAR,
  Head_of_state VARCHAR,
  Capital INT,
  Code2 VARCHAR
) ZONE storageZone STORAGE PROFILE 'default';

-- Table with composite primary key and zone
CREATE TABLE IF NOT EXISTS city (
  Id INT,
  Name VARCHAR,
  Country_code VARCHAR,
  District VARCHAR,
  Population INT,
  PRIMARY KEY (Id, Country_code)
) ZONE storageZone STORAGE PROFILE 'default';

-- Table with composite primary key
CREATE TABLE IF NOT EXISTS countryLanguage (
  Country_code VARCHAR,
  Language VARCHAR,
  Is_official VARCHAR,
  Percentage DECIMAL(4,1),
  PRIMARY KEY (Country_code, Language)
) ZONE storageZone STORAGE PROFILE 'default';

-- Table with default values
-- (Recreate person table with default values)
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (
  Id INT PRIMARY KEY,
  City_id INT DEFAULT 1,
  Name VARCHAR,
  Age INT,
  Company VARCHAR
);

-- Table with UUID generation and defaults
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (
  Id UUID DEFAULT rand_uuid,
  City_id INT DEFAULT 1,
  Name VARCHAR,
  Age INT,
  Company VARCHAR,
  Duration TIMESTAMP WITH LOCAL TIME ZONE DEFAULT CURRENT_TIMESTAMP + INTERVAL '7' DAYS,
  PRIMARY KEY (Id, Duration)
);

-- Table with expiration
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (
  Id INT PRIMARY KEY,
  City_id INT DEFAULT 1,
  Name VARCHAR(10),
  Age INT,
  Company VARCHAR,
  Ttl TIMESTAMP WITH LOCAL TIME ZONE
) EXPIRE AT Ttl;

-- ============================================================================
-- CREATE INDEX EXAMPLES
-- ============================================================================

-- Basic index
CREATE INDEX IF NOT EXISTS idx_country_code ON city (Country_code);

-- Index with DESC and ASC ordering
CREATE INDEX IF NOT EXISTS department_name_idx ON person (City_id DESC, Name ASC);

-- Hash index
CREATE INDEX IF NOT EXISTS name_surname_idx ON person USING HASH (Name, Age);

-- Sorted index
CREATE INDEX IF NOT EXISTS department_city_idx ON person USING SORTED (Age ASC, City_id DESC);

-- Index on CountryLanguage table
CREATE INDEX IF NOT EXISTS idx_lang_country_code ON countryLanguage (Country_code);

-- ============================================================================
-- ALTER TABLE EXAMPLES
-- ============================================================================

-- Add column with IF EXISTS check
ALTER TABLE IF EXISTS person ADD Number BIGINT;

-- Add multiple columns
ALTER TABLE person ADD COLUMN (Code VARCHAR, Gdp DOUBLE);

-- Drop single column
ALTER TABLE person DROP COLUMN Company;

-- Drop column with IF EXISTS check
ALTER TABLE IF EXISTS person DROP COLUMN Number;

-- Drop multiple columns
ALTER TABLE person DROP COLUMN (Code, Gdp);

-- Alter column data type
ALTER TABLE person ALTER COLUMN Age SET DATA TYPE BIGINT;

-- Drop NOT NULL constraint
ALTER TABLE person ALTER COLUMN Name DROP NOT NULL;

-- Set default value for column
ALTER TABLE person ALTER COLUMN City_id SET DEFAULT 1;

-- Drop default value
ALTER TABLE person ALTER COLUMN City_id DROP DEFAULT;

-- Set table expiration
ALTER TABLE person SET EXPIRE AT Ttl;

-- Drop table expiration
ALTER TABLE person DROP EXPIRE;


-- ============================================================================
-- DROP EXAMPLES
-- ============================================================================

-- Drop index
DROP INDEX IF EXISTS department_name_idx;

-- Drop table
DROP TABLE IF EXISTS person;

-- Drop schema with RESTRICT (default behavior)
DROP SCHEMA IF EXISTS testSchema RESTRICT;

-- Drop schema with CASCADE (drops all objects in schema)
-- DROP SCHEMA IF EXISTS testSchema CASCADE;

-- Drop zone
DROP ZONE IF EXISTS renamedZone;

-- ============================================================================
-- SAMPLE DATA INSERTION (for demonstration)
-- ============================================================================

INSERT INTO country (Code, Name, Continent, Region, Surface_area, Indep_year, Population, Life_expectancy, Gnp, Gnp_old, Local_name, Government_form, Head_of_state, Capital, Code2)
VALUES
('USA', 'United States', 'North America', 'North America', 9629091.00, 1776, 331000000, 78.9, 21427700.00, 20544343.00, 'United States', 'Federal Republic', 'President', 1, 'US'),
('CAN', 'Canada', 'North America', 'North America', 9984670.00, 1867, 38000000, 82.0, 1736425.00, 1653043.00, 'Canada', 'Constitutional Monarchy', 'Prime Minister', 2, 'CA');

INSERT INTO city (Id, Name, Country_code, District, Population)
VALUES
(1, 'Washington', 'USA', 'District of Columbia', 705749),
(2, 'Ottawa', 'CAN', 'Ontario', 994837),
(3, 'New York', 'USA', 'New York', 8175133),
(4, 'Toronto', 'CAN', 'Ontario', 2731571);

INSERT INTO countryLanguage (Country_code, Language, Is_official, Percentage)
VALUES
('USA', 'English', 'T', 82.1),
('USA', 'Spanish', 'F', 10.7),
('CAN', 'English', 'T', 58.0),
('CAN', 'French', 'T', 22.0);