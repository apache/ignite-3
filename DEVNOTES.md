* [Prerequisites](#prerequisites)
* [Building Ignite](#building-ignite)
* [Running sanity checks](#running-sanity-checks)
* [Running tests](#running-tests)
* [Checking and generating Javadoc](#checking-and-generating-javadoc)
* [Setting up IntelliJ Idea project](#setting-up-intellij-idea-project)
* [Code structure](#code-structure)
* [Release candidate verification](#release-candidate-verification)
***

# Deprecated Maven build

The maven is depricated and will be removed soon. Gradle is a primary build tool, please follow instructions for that.

## Prerequisites
 * Java 11 SDK
 * Maven 3.6.0+ (for building)
***


## Building Ignite
Ignite follows standard guidelines for multi-module maven projects, so it can be easily built using the following command from the project root directory (you can disable the tests when building using `-DskipTests`):
```
mvn clean package [-DskipTests]
```
Upon build completion, CLI tool can be found be under `modules/cli/target` directory. Use `ignite` on Linux and MacOS, or `ignite.exe` on Windows.
***


## Running sanity checks
### Code style
Code style is checked with [Apache Maven Checkstyle Plugin](https://maven.apache.org/plugins/maven-checkstyle-plugin/).
* [Checkstyle rules](check-rules/checkstyle-rules.xml)
* [Checkstyle suppressions](check-rules/checkstyle-suppressions.xml)
* [Checkstyle rules for javadocs](https://checkstyle.sourceforge.io/config_javadoc.html)

It is enabled by default and is bound to `compile` phase.

Build project without code style check:
```
mvn clean <compile|package|install|deploy> -Dcheckstyle.skip
```

Run code style checks only:
```
mvn clean validate -Pcheckstyle -Dmaven.all-checks.skip
```

Run javadoc style checks for public api only:
```
mvn clean checkstyle:checkstyle-aggregate -P javadoc-public-api
```
>ℹ `javadoc-public-api` profile is required for enabling checkstyle rules for public API javadoc.

Code style check results are generated at:
* `target/site/checkstyle-aggregate.html`
* `target/checkstyle.xml`

### Legacy API
The project is checked for legacy APIs with [Modernizer Maven Plugin](https://github.com/gaul/modernizer-maven-plugin/).
* [Modernizer rules](check-rules/modernizer-rules.xml)

Plugin is enabled by default and is bound to `test-compile` phase (due to requirement to run on already compiled classes)

Build project without legacy API check:
```
mvn clean <compile|package|install|deploy> -Dmodernizer.skip
```

Run legacy API checks only:
```
mvn clean test-compile -Pmodernizer -Dmaven.all-checks.skip
```
or
```
mvn clean test-compile -Dmaven.all-checks.skip && mvn modernizer:modernizer
```

### License headers
Project files license headers match with required template is checked with [Apache Rat Maven Plugin](https://creadur.apache.org/rat/apache-rat-plugin/).
```
mvn clean apache-rat:check -pl :apache-ignite
```
License headers check result is generated at `target/rat.txt`

### PMD
Static code analyzer is run with [Apache Maven PMD Plugin](https://maven.apache.org/plugins/maven-pmd-plugin/). Precompilation is required 'cause PMD shoud be run on compiled code.
* [PMD rules](check-rules/pmd-rules.xml)
```
mvn clean compile pmd:check
```
PMD check result (only if there are any violations) is generated at `target/pmd.xml`.
***

### Maven
Project is supplied with number of custom scripts for Maven sanity check.
To run checks, execute:
```
bash check-rules/maven-check-scripts/run.sh
```
from root of the project.

Linux, MacOS, WSL (Windows Subsystem on Linux) or alike environment is required.
`xpath` should be present in PATH
***


## Running tests
Run unit tests only:
```
mvn test
```
Run unit + integration tests:
```
mvn integration-test
```
Run integration tests only:
```
mvn integration-test -Dskip.surefire.tests
```
***


## Checking and generating Javadoc
Javadoc is generated and checked for correctness with [Maven Javadoc Plugin](https://maven.apache.org/plugins/maven-javadoc-plugin/).
(Javadoc style check is described above in [Code style](#code-style) section)

Check Javadoc is correct (precompilation is required for resolving internal dependencies):
```
mvn compile javadoc:javadoc 
```
Build Javadoc jars (found in `target` directory of module):
```
mvn package -P javadoc -Dmaven.test.skip
```
Build Javadoc site (found in `target/site/apidocs/index.html`):
```
mvn compile javadoc:aggregate -P javadoc
```
>ℹ `javadoc` profile is required for excluding internal classes
***


## Setting up IntelliJ Idea project
You can quickly import Ignite project to your IDE using the root `pom.xml` file. In IntelliJ, choose `Open Project` from the `Quick Start` box or choose `Open...` from the `File` menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:
 * Open the `File` menu and select `Project Structure...`
 * In the SDKs section, ensure that a 1.11 JDK is selected (create one if none exist)
 * In the `Project` section, make sure the project language level is set to 11.0 as Ignite makes use of several Java 11
 language features

Ignite uses machine code generation for some of it's modules. To generate necessary production code, build the project using maven (see [Building Ignite](#building-ignite)).

If you want to make use of Idea build action and incremental compilation, you have to alter the build process manually
as Idea Maven integration doesn't support all the Maven plugins out of the box.
 * Open Maven tab
 * Click on the gear and untick `Show Basic Phases Only`
 * Find `ignite-sql-engine` module
 * Select `Lifecycle` -> `process-resources`
 * Open contextual menu and select `Before Build`

***


## Code structure
High-level modules structure and detailed modules description can be found in the [modules readme](modules/README.md).
***


## Release candidate verification
1. Build the package (this will also run unit tests and the license headers check)
    ```
    mvn clean package
    ```
1. Go to the `modules/cli/target` directory which now contains the packaged CLI tool
    ```
    cd modules/cli/target
    ```
1. Run the tool without parameters (full list of available commands should appear)
    ```
    ./ignite
    ```
1. Run the initialization step
    ```
    ./ignite init --repo=<path to Maven staging repository>
    ```
1. Install an additional dependency (Guava is used as an example)
    ```
    ./ignite module add mvn:com.google.guava:guava:23.0
    ```
1. Verify that Guava has been installed
    ```
    ./ignite module list
    ```
1. Start a node
    ```
    ./ignite node start myFirstNode
    ```
1. Check that the new node is up and running
    ```
    ./ignite node list
    ```
1. Stop the node
    ```
    ./ignite node stop myFirstNode
    ```

# Gradle build

## Prerequisites
* Java 11 SDK
***


## Building Ignite
Ignite follows standard guidelines for multi-module gradle projects, so it can be easily built using the following command from the project 
root directory (you can disable the tests when building using `-x test`):
```shell
./gradlew clean build -x test
```
***


## Running sanity checks

Run all checks:
```shell
./gradlew clean check
```

Skip all checks:
```shell
./gradlew clean build -x check
```

### Code style
Code style is checked with [Gradle Checkstyle Plugin](https://docs.gradle.org/current/userguide/checkstyle_plugin.html).
* [Checkstyle rules](check-rules/checkstyle-rules.xml)
* [Checkstyle suppressions](check-rules/checkstyle-suppressions.xml)
* [Checkstyle rules for javadocs](https://checkstyle.sourceforge.io/config_javadoc.html)

It is enabled by default and is bound to `check` task.

Build project without code style check:
```shell
./gradlew clean build -x checkstyleMain -x checkstyleIntegrationTest -x checkstyleTest -x checkstyleTestFixtures
```

Run code style checks only:
```shell
./gradlew checkstyleMain checkstyleIntegrationTest checkstyleTest checkstyleTestFixtures
```

Code style check results are generated at:
* `<mudule-dir>/build/reports/checkstyle/`

### Legacy API
The project is checked for legacy APIs with [Modernizer Gradle Plugin](https://plugins.gradle.org/plugin/com.github.andygoossens.gradle-modernizer-plugin).
* [Modernizer rules](check-rules/modernizer-rules.xml)

Plugin is enabled by default and is bound to `build` task.

Build project without legacy API check:
```shell
./gradlew clean build -x modernizer
```

Run legacy API checks only:
```shell
./gradlew clean modernizer
```

### PMD
Static code analyzer is run with [Apache Gradle PMD Plugin](https://docs.gradle.org/current/userguide/pmd_plugin.html).
* [PMD rules](check-rules/pmd-rules.xml)
```shell
./gradlew clean pmdMain pmdTest pmdTestFixtures pmdIntegrationTest
```
PMD check result (only if there are any violations) is generated at `<module-name>/build/reports/pmd/`.
***


## Running tests
Run unit tests only:
```shell
./gradlew clean test
```
Run unit + integration tests:
```shell
./gradlew clean test integrationTest
```
Run integration tests only:
```shell
./gradlew clean integrationTest
```
***

## Checking and generating Javadoc
Javadoc is generated and checked for correctness with [Gradle Javadoc Plugin](https://docs.gradle.org/current/dsl/org.gradle.api.tasks.javadoc.Javadoc.html).

Build Javadoc site (found in `build/docs/aggregateJavadoc/index.html`):
```
./gradlew aggregateJavadoc 
```

If you don't need to aggregate all javadoc you can use javadoc task and find generated 
artifacts in each module (for example `modules/api/build/docs/javadoc`)
```
./gradlew javadoc
```
***


## Setting up IntelliJ Idea project
You can quickly import Ignite project to your IDE using the root `build.gradle` file. In IntelliJ, choose `Open Project` from the `Quick Start` box or choose `Open...` from the `File` menu and select the root `build.gradle` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:
* Open the `File` menu and select `Project Structure...`
* In the SDKs section, ensure that JDK 11 is selected (create one if none exist)
* In the `Project` section, make sure the project language level is set to 11.0 as Ignite makes use of several Java 11
  language features

Ignite uses machine code generation for some of it's modules. To generate necessary production code, build the project using gradle.

Configure Idea code style (for IntelliJ Idea >= 2019):
* File -> Settings -> Editor -> Code Style -> Scheme -> gear (Show Scheme Actions) -> Import Scheme -> IntelliJ IDEA code style XML
* Choose: ${igniteProject}/idea/intellij-java-google-style.xml
* Import schema
* Reboot IntelliJ Idea
***

## Code structure
High-level modules structure and detailed modules description can be found in the [modules readme](modules/README.md).
***

## Packaging

### Zip packaging
```shell
./gradlew clean allDistZip -x test -x check
```
Uber zip package will be located in `packaging/build/distributions`.

If you wand to build CLI, you can do it with:
```shell
./gradlew clean packaging-cli:distZip -x test -x check
```
Zip package will be located in `packaging/cli/build/distributions`.

For ignite-runner:
```shell
./gradlew clean packaging-db:distZip -x test -x check
```
Zip package will be located in `packaging/db/build/distributions`.

You can build zip and run CLI with the following commands:
```shell
./gradlew clean packaging-cli:distZip -x test -x check
cd packaging/cli/build/distributions
unzip ignite3-cli-<version>
cd ignite3-cli-<version>
./bin/ignite3
```

To build a zip file with ignite-runner and run it:
```shell
./gradlew clean packaging-db:distZip -x test -x check
cd packaging/db/build/distributions
unzip ignite3-db-<version>
cd ignite3-db-<version>
./bin/ignite3db start
```

To stop the started node run:
```shell
./bin/ignite3db stop
```

### RPM/DEB packaging

There is also RPM/DEB packaging for Ignite. To build those packages run:
```shell
./gradlew clean buildDeb buildRpm -x test -x check
```
`ignite3-cli` packages are located in `packaging/cli/build/distributions/` and `ignite3-db` packages in `packaging/db/build/distributions/`.
***

To install RPM packages run:
```shell
rpm -i ignite3-cli-<version>.noarch.rpm
rpm -i ignite3-db-<version>.noarch.rpm
```

To install DEB packages run:
```shell
dpkg --install ignite3-cli_<version>_all.deb
dpkg --install ignite3-db_<version>_all.deb
```

Run ignite3db service:
```shell
service ignite3db start
```

Stop ignite3db service:
```shell
service ignite3db stop
```

Use CLI:
```shell
ignite3
```

To uninstall RPM packages run:
```shell
rpm -e ignite3-cli
rpm -e ignite3-db
```

To uninstall DEB packages run:
```shell
dpkg --remove ignite3-cli
dpkg --remove ignite3-db
```

### Docker image

Gradle build also provides the task that can build docker image. To run this task make sure you have docker installed.
```shell
./gradlew clean docker -x test -x check
```

Run docker container with ignite node:
```shell
docker run -it --rm -p 10300:10300 apacheignite/ignite3
```

There's a sample docker compose file which allows to run 3 nodes in a cluster in the `packaging/docker` directory. You can also use CLI from
the docker image using `cli` parameter and connect to nodes using their names from the docker network.
```shell
docker compose -f packaging/docker/docker-compose.yml -d up
docker run -it --rm --net ignite3_default apacheignite/ignite3 cli
> connect http://node1:10300
> cluster init --cluster-name cluster --meta-storage-node node1 --meta-storage-node node2 --meta-storage-node node3
```

## Release candidate verification
1. Build all packages (this will also run unit tests and all checks)
    ```shell
    ./gradlew clean docker distZip allDistZip buildRpm buildDeb
    ```
1. Go to the `packaging/build/distributions` directory which now contains the packaged CLI tool and Ignite
    ```shell
   cd packaging/build/distributions
   unzip ignite3-<version> 
    ```
1. Run the tool without parameters (full list of available commands should appear)
    ```shell
   cd ignite3-cli-<version>
   ./bin/ignite3
    ```
1. Start a node
    ```shell
   cd ../ignite3-db-<version>
   ./bin/ignite3db start
    ```
1. Check that the new node is up and running
    ```shell
    cd ../ignite3-cli-<version>
   ./bin/ignite3 node status
    ```
1. Stop the node
    ```shell
    cd ../ignite3-db-<version>
   ./bin/ignite3db stop
    ```
