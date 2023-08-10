* [Prerequisites](#prerequisites)
* [Quick Start](#quick-start)
* [Building Ignite](#building-ignite)
* [Running sanity checks](#running-sanity-checks)
* [Running tests](#running-tests)
* [Checking and generating Javadoc](#checking-and-generating-javadoc)
* [Setting up IntelliJ Idea project](#setting-up-intellij-idea-project)
* [Use prepared IntelliJ Idea run configurations](#use-prepared-idea-run-configurations)
* [Code structure](#code-structure)
* [Release candidate verification](#release-candidate-verification)
***


## Prerequisites
 * Java 11 SDK
***


## Quick Start
Apache Ignite 3 follows standard guidelines for multi-module Gradle projects, so it can be built by using the following command from the
project root directory:
```shell
./gradlew clean build
```
This command builds a project and performs a few additional actions, for example it also runs tests. The build runs faster if
these actions are disabled as described in the next section.

To start an ignite-3 instance, package Apache Ignite 3 as described below and then follow [the user guide](https://ignite.apache.org/docs/3.0.0-beta/quick-start/getting-started-guide).
***


## Building Ignite
Apache Ignite 3 follows standard guidelines for multi-module Gradle projects, so it can be built by using the following command from the
project root directory (the tests are disabled with `-x test -x integrationTest` options):
```shell
./gradlew clean build -x test -x integrationTest
```
For a really fast build some other actions can be disabled too:
```shell
./gradlew clean build -x assembleDist -x distTar -x distZip -x check
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
./gradlew modernizer
```

### PMD
Static code analyzer is run with [Apache Gradle PMD Plugin](https://docs.gradle.org/current/userguide/pmd_plugin.html).
* [PMD rules](check-rules/pmd-rules.xml)
```shell
./gradlew pmdMain
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
You can quickly import Ignite project to your IDE using the root `build.gradle` file. In IntelliJ, choose `Open Project` from the
`Quick Start` box or choose `Open...` from the `File` menu and select the root `build.gradle` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:
* Open the `File` menu and select `Project Structure...`
* In the SDKs section, ensure that JDK 11 is selected (create one if none exist)
* In the `Project` section, make sure the project language level is set to 11.0 as Ignite makes use of several Java 11
  language features

Apache Ignite 3 uses machine code generation for some of its modules. Occasionally, IDEs may fail to trigger this code generation. In
this case, run a gradle build command from the command line. Subsequent builds can be performed from IDE without problems.
***

## Use prebuilt IntelliJ Idea run configurations
The Apache Ignite 3 project contains prebuilt IntelliJ Idea run configurations that can be useful in common cases. 

![img.png](docs/img.png)

These configurations are stored in `.run` root folder and committed to GIT repo.

***NOTE: DO NOT MODIFY THESE CONFIGURATION FILES MANUALLY.***

For modification use Idea `Edit Configurations...` option.
***

## Code structure
High-level modules structure and detailed modules description can be found in the [modules readme](modules/README.md).
***

## Packaging

### Zip packaging
```shell
./gradlew clean allDistZip -x check
```
Uber zip package will be located in `packaging/build/distributions`.

If you wand to build CLI, you can do it with:
```shell
./gradlew clean packaging-cli:distZip -x check
```
Zip package will be located in `packaging/cli/build/distributions`.

For ignite-runner:
```shell
./gradlew clean packaging-db:distZip -x check
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
./gradlew clean buildDeb buildRpm -x check
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
docker run -it --rm -p 10300:10300 -p 10800:10800 apacheignite/ignite3
```

There's a sample docker compose file which allows to run 3 nodes in a cluster in the `packaging/docker` directory. You can also use CLI from
the docker image using `cli` parameter and connect to nodes using their names from the docker network.
```shell
docker compose -f packaging/docker/docker-compose.yml up -d
docker run -it --rm --net ignite3_default apacheignite/ignite3 cli
> connect http://node1:10300
> cluster init --cluster-name cluster --meta-storage-node node1 --meta-storage-node node2 --meta-storage-node node3
```

## Release candidate verification
1. Build all packages (this will also run unit tests and all checks)
    ```shell
    ./gradlew clean docker distZip allDistZip buildRpm buildDeb
    ```
2. Go to the `packaging/build/distributions` directory which now contains the packaged CLI tool and Ignite
    ```shell
   cd packaging/build/distributions
   unzip ignite3-<version> 
    ```
3. Run the tool without parameters (full list of available commands should appear)
    ```shell
   cd ignite3-cli-<version>
   ./bin/ignite3
    ```
4. Start a node
    ```shell
   cd ../ignite3-db-<version>
   ./bin/ignite3db start
    ```
5. Check that the new node is up and running
    ```shell
    cd ../ignite3-cli-<version>
   ./bin/ignite3 node status
    ```
6. Stop the node
    ```shell
    cd ../ignite3-db-<version>
   ./bin/ignite3db stop
    ```
