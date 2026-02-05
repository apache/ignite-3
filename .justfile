dist_name := 'ignite3-3.2.0-SNAPSHOT'
dist_db_name := 'ignite3-db-3.2.0-SNAPSHOT'
dist_cli_name := 'ignite3-cli-3.2.0-SNAPSHOT'

cli_dir := 'ignite3-cli'
cli_name := 'ignite3'
node_name_prefix := 'ignite3-db'
node_name := 'ignite3db'
config_name := 'ignite-config.conf'

_change_node_name idx:
    sed -i '' 's/NODE_NAME=defaultNode/NODE_NAME=node{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/vars.env

_increment_ports idx:
  sed -i '' 's/port=10300/port=1030{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}
  sed -i '' 's/port=3344/port=330{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}
  sed -i '' 's/port=10800/port=1080{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}

  sed -i '' '/netClusterNodes=\[/,/\]/s/"localhost:3344"/"localhost:3301", "localhost:3302", "localhost:3303"/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}

_cp_db idx:
  cp -r w/{{dist_db_name}} w/{{node_name_prefix}}-{{idx}}

# Build and update CLI only (faster than full setup)
setup_cli:
  ./gradlew :packaging-cli:distZip -x test -x check
  rm -rf w/{{cli_dir}}
  unzip -q packaging/cli/build/distributions/{{dist_cli_name}}.zip -d w/
  mv w/{{dist_cli_name}} w/{{cli_dir}}

# Start the CLI
cli:
  w/{{cli_dir}}/bin/{{cli_name}}

# Start the CLI with remote debug on port 5111
cli_debug:
  JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5111" w/{{cli_dir}}/bin/{{cli_name}}

# Start the CLI with remote debug on port 5111 (alternative using IGNITE3_OPTS)
cli_debug_alt:
  IGNITE3_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5111" w/{{cli_dir}}/bin/{{cli_name}}

# Start a specific node by index
start idx:
  w/{{node_name_prefix}}-{{idx}}/bin/{{node_name}} start

# Stop a specific node by index
stop idx:
  w/{{node_name_prefix}}-{{idx}}/bin/{{node_name}} stop

# Start all nodes
start_all:
  just start 1
  just start 2
  just start 3

# Stop all nodes
stop_all:
    just stop 1
    just stop 2
    just stop 3

# Clean workspace and build artifacts
clean:
  rm -rf w
  ./gradlew clean

# Initialize cluster (works with 1 or more nodes)
init:
  w/{{cli_dir}}/bin/{{cli_name}} cluster init --name myCluster --metastorage-group node1 --url http://localhost:10300

# Set up local 3-node cluster
setup:
  rm -rf w

  mkdir w

  ./gradlew allDistZip

  unzip packaging/build/distributions/{{dist_name}}.zip -d w

  just _cp_db 1
  just _cp_db 2
  just _cp_db 3

  mv w/{{dist_cli_name}} w/{{cli_dir}}

  just _change_node_name 1
  just _change_node_name 2
  just _change_node_name 3

  just _increment_ports 1
  just _increment_ports 2
  just _increment_ports 3

  rm -rf w/{{dist_db_name}}

# Build project
build:
  ./gradlew clean build

# Build project (fast - skip tests)
build_fast:
  ./gradlew clean build -x test -x integrationTest

# Build project (fastest - skip all checks)
build_fastest:
  ./gradlew clean build -x assembleDist -x distTar -x distZip -x check

# Run unit tests
test:
  ./gradlew clean test

# Run integration tests
test_integration:
  ./gradlew clean integrationTest

# Run all tests
test_all:
  ./gradlew clean test integrationTest

# Run code quality checks
check:
  ./gradlew clean check

# Run checkstyle
checkstyle:
  ./gradlew checkstyleMain checkstyleIntegrationTest checkstyleTest checkstyleTestFixtures

# Run spotbugs
spotbugs:
  ./gradlew spotbugsMain

# Run PMD
pmd:
  ./gradlew pmdMain pmdTest

# Generate javadoc
javadoc:
  ./gradlew aggregateJavadoc

# Build docker image
docker:
  ./gradlew clean docker -x test -x check

# Start docker compose cluster
docker_up:
  docker compose -f packaging/docker/docker-compose.yml up -d

# Stop docker compose cluster
docker_down:
  docker compose -f packaging/docker/docker-compose.yml down

# Run JMH benchmarks for a specific module
bench module:
  ./gradlew clean :ignite-{{module}}:jmh

# Show status of all nodes
status:
  #!/bin/bash
  echo "Node 1 status:"
  w/{{node_name_prefix}}-1/bin/{{node_name}} status || echo "Node 1 not running"
  echo "Node 2 status:"
  w/{{node_name_prefix}}-2/bin/{{node_name}} status || echo "Node 2 not running"
  echo "Node 3 status:"
  w/{{node_name_prefix}}-3/bin/{{node_name}} status || echo "Node 3 not running"

# Complete setup and cluster initialization
setup_cluster: setup start_all init

# List all available recipes
default:
  @just --list