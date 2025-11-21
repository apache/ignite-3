-- Drop tables first (zones cannot be dropped while tables use them)
DROP TABLE IF EXISTS Person;
DROP TABLE IF EXISTS Department;
DROP TABLE IF EXISTS Employee;
DROP TABLE IF EXISTS MY_TABLE;

-- Drop distribution zones
DROP ZONE IF EXISTS renamedZone;
DROP ZONE IF EXISTS "myExampleZone";
DROP ZONE IF EXISTS autoScaleUpZone;
DROP ZONE IF EXISTS autoScaleDownZone;
DROP ZONE IF EXISTS highAvailabilityZone;

-- Create a case-insensitive distribution zone (exampleZone)
CREATE ZONE IF NOT EXISTS exampleZone STORAGE PROFILES['default'];

-- Create a case-sensitive distribution zone ("myExampleZone")
CREATE ZONE IF NOT EXISTS "myExampleZone" STORAGE PROFILES['default'];

-- Create a zone that waits 300 seconds before adding new nodes
CREATE ZONE IF NOT EXISTS autoScaleUpZone (AUTO SCALE UP 300) STORAGE PROFILES['default'];

-- Create a zone that waits 600 seconds before removing idle nodes
CREATE ZONE IF NOT EXISTS autoScaleDownZone (AUTO SCALE DOWN 600) STORAGE PROFILES['default'];

-- The following example requires nodes to be configured with
-- the 'storage' attribute set to 'SSD'. To configure node attributes,
-- use the GridGain CLI or configuration files.
-- CREATE ZONE IF NOT EXISTS ssdOnlyZone (NODES FILTER '$[?(@.storage == "SSD")]') STORAGE PROFILES['default'];

-- Create a zone with high availability mode and 5 replicas
CREATE ZONE IF NOT EXISTS highAvailabilityZone (
  REPLICAS 5,
  CONSISTENCY MODE 'HIGH_AVAILABILITY'
) STORAGE PROFILES['default'];

-- Rename exampleZone to renamedZone
ALTER ZONE IF EXISTS exampleZone RENAME TO renamedZone;

-- Set the number of replicas for renamedZone to 10
ALTER ZONE IF EXISTS renamedZone SET REPLICAS 10;

-- Set multiple parameters at once: replicas, quorum size, and node filter
ALTER ZONE IF EXISTS highAvailabilityZone SET (
  REPLICAS 5,
  QUORUM SIZE 3
);

-- Create a Person table using the renamed zone
CREATE TABLE IF NOT EXISTS Person (
  id INT PRIMARY KEY,
  city_id INT,
  name VARCHAR,
  age INT,
  company VARCHAR
) ZONE renamedZone;

-- Create a Department table using the high availability zone
CREATE TABLE IF NOT EXISTS Department (
  dept_id INT PRIMARY KEY,
  dept_name VARCHAR,
  location VARCHAR
) ZONE highAvailabilityZone;

-- Create a table using the case-sensitive zone name
CREATE TABLE IF NOT EXISTS Employee (
  emp_id INT PRIMARY KEY,
  dept_id INT,
  emp_name VARCHAR,
  salary DECIMAL
) ZONE "myExampleZone";

-- Show all distribution zones
SELECT * FROM SYSTEM.ZONES;

-- Show specific zone details
SELECT * FROM SYSTEM.ZONES WHERE NAME = 'renamedZone';