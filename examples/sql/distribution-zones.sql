-- Drop tables first (zones cannot be dropped while tables use them)
DROP TABLE IF EXISTS person;
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS my_table;

-- Drop distribution zones
DROP ZONE IF EXISTS renamed_zone;
DROP ZONE IF EXISTS "my_example_zone";
DROP ZONE IF EXISTS auto_scale_up_zone;
DROP ZONE IF EXISTS auto_scale_down_zone;
DROP ZONE IF EXISTS high_availability_zone;

-- Create a case-insensitive distribution zone (example_zone)
CREATE ZONE IF NOT EXISTS example_zone STORAGE PROFILES['default'];

-- Create a case-sensitive distribution zone ("my_example_zone")
CREATE ZONE IF NOT EXISTS "my_example_zone" STORAGE PROFILES['default'];

-- Create a zone that waits 300 seconds before adding new nodes
CREATE ZONE IF NOT EXISTS auto_scale_up_zone (AUTO SCALE UP 300) STORAGE PROFILES['default'];

-- Create a zone that waits 600 seconds before removing idle nodes
CREATE ZONE IF NOT EXISTS auto_scale_down_zone (AUTO SCALE DOWN 600) STORAGE PROFILES['default'];

-- The following example requires nodes to be configured with
-- the 'storage' attribute set to 'SSD'. To configure node attributes,
-- use the Ignite CLI or configuration files.
-- CREATE ZONE IF NOT EXISTS ssd_only_zone (NODES FILTER '$[?(@.storage == "SSD")]') STORAGE PROFILES['default'];

-- Create a zone with high availability mode and 5 replicas
CREATE ZONE IF NOT EXISTS high_availability_zone (
  REPLICAS 5,
  CONSISTENCY MODE 'HIGH_AVAILABILITY'
) STORAGE PROFILES['default'];

-- Rename example_zone to renamed_zone
ALTER ZONE IF EXISTS example_zone RENAME TO renamed_zone;

-- Set the number of replicas for renamed_zone to 10
ALTER ZONE IF EXISTS renamed_zone SET REPLICAS 10;

-- Set multiple parameters at once: replicas, quorum size, and node filter
ALTER ZONE IF EXISTS high_availability_zone SET (
  REPLICAS 5,
  QUORUM SIZE 3
);

-- Create a person table using the renamed zone
CREATE TABLE IF NOT EXISTS person (
  id INT PRIMARY KEY,
  city_id INT,
  name VARCHAR,
  age INT,
  company VARCHAR
) ZONE renamed_zone;

-- Create a department table using the high availability zone
CREATE TABLE IF NOT EXISTS department (
  dept_id INT PRIMARY KEY,
  dept_name VARCHAR,
  location VARCHAR
) ZONE high_availability_zone;

-- Create a table using the case-sensitive zone name
CREATE TABLE IF NOT EXISTS employee (
  emp_id INT PRIMARY KEY,
  dept_id INT,
  emp_name VARCHAR,
  salary DECIMAL
) ZONE "my_example_zone";

-- Show all distribution zones
SELECT * FROM system.zones;

-- Show specific zone details
SELECT * FROM system.zones WHERE name = 'renamed_zone';