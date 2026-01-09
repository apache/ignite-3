dist_name := 'ignite3-3.2.0-SNAPSHOT'
dist_db_name := 'ignite3-db-3.2.0-SNAPSHOT'
dist_cli_name := 'ignite3-cli-3.2.0-SNAPSHOT'

cli_dir := 'ignite3-cli'
cli_name := if os_family() == "windows" { "ignite3.bat" } else { "ignite3" }
node_name := if os_family() == "windows" { "ignite3db.bat" } else { "ignite3db" }
node_name_prefix := 'ignite3-db'
config_name := 'ignite-config.conf'

gradlew := if os_family() == "windows" { ".\\gradlew.bat" } else { "./gradlew" }

# Set PowerShell as the shell on Windows
set windows-shell := ["powershell", "-NoProfile", "-Command"]

[unix]
_change_node_name idx:
    sed -i '' 's/NODE_NAME=defaultNode/NODE_NAME=node{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/vars.env

[windows]
_change_node_name idx:
    (Get-Content 'w/{{node_name_prefix}}-{{idx}}/etc/vars.bat') -replace 'NODE_NAME=defaultNode', 'NODE_NAME=node{{idx}}' | Set-Content 'w/{{node_name_prefix}}-{{idx}}/etc/vars.bat'

[unix]
_increment_ports idx:
    sed -i '' 's/port=10300/port=1030{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}
    sed -i '' 's/port=3344/port=330{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}
    sed -i '' 's/port=10800/port=1080{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}
    sed -i '' '/netClusterNodes=\[/,/\]/s/"localhost:3344"/"localhost:3301", "localhost:3302", "localhost:3303"/' w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}

[windows]
_increment_ports idx:
    $f = 'w/{{node_name_prefix}}-{{idx}}/etc/{{config_name}}'; $c = Get-Content $f; $c = $c -replace 'port=10300', 'port=1030{{idx}}'; $c = $c -replace 'port=3344', 'port=330{{idx}}'; $c = $c -replace 'port=10800', 'port=1080{{idx}}'; $c = $c -replace '"localhost:3344"', '"localhost:3301", "localhost:3302", "localhost:3303"'; Set-Content $f $c

[unix]
_cp_db idx:
    cp -r w/{{dist_db_name}} w/{{node_name_prefix}}-{{idx}}

[windows]
_cp_db idx:
    Copy-Item -Path 'w/{{dist_db_name}}' -Destination 'w/{{node_name_prefix}}-{{idx}}' -Recurse

[unix]
_rm_rf path:
    rm -rf {{path}}

[windows]
_rm_rf path:
    if (Test-Path '{{path}}') { Remove-Item -Path '{{path}}' -Recurse -Force }

[unix]
_mkdir path:
    mkdir -p {{path}}

[windows]
_mkdir path:
    New-Item -ItemType Directory -Path '{{path}}' -Force | Out-Null

[unix]
_unzip src dest:
    unzip -q {{src}} -d {{dest}}

[windows]
_unzip src dest:
    Expand-Archive -Path '{{src}}' -DestinationPath '{{dest}}' -Force

[unix]
_mv src dest:
    mv {{src}} {{dest}}

[windows]
_mv src dest:
    Move-Item -Path '{{src}}' -Destination '{{dest}}' -Force

# Build and update CLI only (faster than full setup)
setup_cli:
    {{gradlew}} :packaging-cli:distZip -x test -x check
    just _rm_rf w/{{cli_dir}}
    just _unzip packaging/cli/build/distributions/{{dist_cli_name}}.zip w/
    just _mv w/{{dist_cli_name}} w/{{cli_dir}}

# Start the CLI
cli:
    w/{{cli_dir}}/bin/{{cli_name}}

# Start the CLI with remote debug on port 5111
[unix]
cli_debug:
    JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5111" w/{{cli_dir}}/bin/{{cli_name}}

[windows]
cli_debug:
    $env:JAVA_OPTS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5111'; w/{{cli_dir}}/bin/{{cli_name}}

# Start the CLI with remote debug on port 5111 (alternative using IGNITE3_OPTS)
[unix]
cli_debug_alt:
    IGNITE3_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5111" w/{{cli_dir}}/bin/{{cli_name}}

[windows]
cli_debug_alt:
    $env:IGNITE3_OPTS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5111'; w/{{cli_dir}}/bin/{{cli_name}}

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
    just _rm_rf w
    {{gradlew}} clean

# Initialize cluster (works with 1 or more nodes)
init:
    w/{{cli_dir}}/bin/{{cli_name}} cluster init --name myCluster --metastorage-group node1 --url http://localhost:10300

# Set up local 3-node cluster
setup:
    just _rm_rf w
    just _mkdir w
    {{gradlew}} allDistZip
    just _unzip packaging/build/distributions/{{dist_name}}.zip w
    just _cp_db 1
    just _cp_db 2
    just _cp_db 3
    just _mv w/{{dist_cli_name}} w/{{cli_dir}}
    just _change_node_name 1
    just _change_node_name 2
    just _change_node_name 3
    just _increment_ports 1
    just _increment_ports 2
    just _increment_ports 3
    just _rm_rf w/{{dist_db_name}}

# Build project
build:
    {{gradlew}} clean build

# Build project (fast - skip tests)
build_fast:
    {{gradlew}} clean build -x test -x integrationTest

# Build project (fastest - skip all checks)
build_fastest:
    {{gradlew}} clean build -x assembleDist -x distTar -x distZip -x check

# Run unit tests
test:
    {{gradlew}} clean test

# Run integration tests
test_integration:
    {{gradlew}} clean integrationTest

# Run all tests
test_all:
    {{gradlew}} clean test integrationTest

# Run code quality checks
check:
    {{gradlew}} clean check

# Run checkstyle
checkstyle:
    {{gradlew}} checkstyleMain checkstyleIntegrationTest checkstyleTest checkstyleTestFixtures

# Run spotbugs
spotbugs:
    {{gradlew}} spotbugsMain

# Run PMD
pmd:
    {{gradlew}} pmdMain pmdTest

# Generate javadoc
javadoc:
    {{gradlew}} aggregateJavadoc

# Build docker image
docker:
    {{gradlew}} clean docker -x test -x check

# Start docker compose cluster
docker_up:
    docker compose -f packaging/docker/docker-compose.yml up -d

# Stop docker compose cluster
docker_down:
    docker compose -f packaging/docker/docker-compose.yml down

# Run JMH benchmarks for a specific module
bench module:
    {{gradlew}} clean :ignite-{{module}}:jmh

# Show status of all nodes
[unix]
status:
    #!/bin/bash
    echo "Node 1 status:"
    w/{{node_name_prefix}}-1/bin/{{node_name}} status || echo "Node 1 not running"
    echo "Node 2 status:"
    w/{{node_name_prefix}}-2/bin/{{node_name}} status || echo "Node 2 not running"
    echo "Node 3 status:"
    w/{{node_name_prefix}}-3/bin/{{node_name}} status || echo "Node 3 not running"

[windows]
status:
    Write-Host "Node 1 status:"; try { & w/{{node_name_prefix}}-1/bin/{{node_name}} status } catch { Write-Host "Node 1 not running" }
    Write-Host "Node 2 status:"; try { & w/{{node_name_prefix}}-2/bin/{{node_name}} status } catch { Write-Host "Node 2 not running" }
    Write-Host "Node 3 status:"; try { & w/{{node_name_prefix}}-3/bin/{{node_name}} status } catch { Write-Host "Node 3 not running" }

# Complete setup and cluster initialization
setup_cluster: setup start_all init

# List all available recipes
default:
    @just --list
