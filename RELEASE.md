# Apache Ignite 3 Release Procedure

This document describes the current procedure for preparing an Ignite 3 release.

## Prerequisites

1. Create a GPG key, upload it to a keyserver, and locate its ID. More details here: https://infra.apache.org/openpgp.html
2. Checkout Apache distribution directories:
   ```
   svn checkout https://dist.apache.org/repos/dist/dev/ignite dist-dev
   svn checkout https://dist.apache.org/repos/dist/release/ignite dist-release
   ```
3. Provide your Apache credentials to Maven (required for uploading to Apache Nexus):
   ```xml
   <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 https://maven.apache.org/xsd/settings-1.0.0.xsd">
     <servers>
       <server>
         <id>apache.staging</id>
         <username>USERNAME</username>
         <password>PASSWORD</password>
       </server>
     </servers>
   </settings>
   ```

For all the commands going forward:
* Replace `{version}` with the version number being released.
* Replace `{rc}` with the current sequential number of the release candidate.
* Replace `{gpg}` with your GPG key ID.
* Replace `{dist.dev}` with the local path to the development distribution directory.
* Replace `{dist.release}` with the local path to the release distribution directory.

## Preparing the Release

1. Go to the project home folder.
2. Create a Git tag:
   ```
   git tag -a {version}-rc{rc} -m "{version}-rc{rc}"
   git push --tags
   ```
3. Setup properties in gradle.properties (local or project) https://docs.gradle.org/current/userguide/build_environment.html
   ```
   signing.keyId=*INSERT KEY HERE LAST 8 CHARS*
   signing.password=*INSERT PASSWORD HERE*
   signing.secretKeyRingFile=*INSERT KEY RING ABSOLUTE PATH HERE*
   ```
   For generate secret key ring file please use follow command
   ```
   gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg
   ```
   Show key id command (you need only last 8 chars from printed key)
   ```
   gpg -K
   ```
4. Specify login\password for staging in gradle.properties
   ```
   staging_user=*INSERT STAGING USERNAME HERE*
   staging_password=*INSERT STAGING PASSWORD HERE*
   ```
5. Build the project, sign the artifact and create a staging repository:
   ```
   ./gradlew publishAllPublicationsToMavenRepository
   ```
6. Login to the Apache Nexus and close the new repository: https://repository.apache.org/#stagingRepositories
7. Create an empty folder under the development distribution directory:
   ```
   rm -rf {dist.dev}/{version}-rc{rc}
   mkdir {dist.dev}/{version}-rc{rc}
   ```
8. Create zip distributions 
   ```
   ./gradlew allDistZip
   ```
9. Sign zip distributions
   ```
   ./gradlew signAllDistZip signCliZip signDbZip
   ```
10. Copy ZIP packages along with checksums and signatures to the development distribution directory:
    ```
    cp packaging/build/distributions/ignite3-{version}.zip packaging/build/distributions/ignite3-{version}.zip.asc \
    packaging/build/distributions/ignite3-{version}.zip.sha512 packaging/build/db/distributions/ignite3db-{version}.zip \
    packaging/build/db/distributions/ignite3db-{version}.zip.asc packaging/build/db/distributions/ignite3db-{version}.zip.sha512 \
    packaging/build/cli/distributions/ignite3cli-{version}.zip packaging/build/cli/distributions/ignite3cli-{version}.zip.asc \
    packaging/build/cli/distributions/ignite3cli-{version}.zip.sha512 \
    /Users/vkulichenko/GridGain/dist-dev/{version}-rc{rc}
    ```
11. Create DEB\RPM distributions, they will be signed 
   ```
   ./gradlew buildDeb buildRpm
   ```
12. Copy DEB\RPM packages to the development distribution directory:
   ```
   cp packaging/db/build/distributions/*.deb packaging/db/build/distributions/*.changes packaging/db/build/distributions/*.rpm \
   packaging/cli/build/distributions/*.deb packaging/cli/build/distributions/*.changes packaging/cli/build/distributions/*.rpm \
   /Users/vkulichenko/GridGain/dist-dev/{version}-rc{rc}
   ```
13. Commit ZIP and DEB\RPM packages:
   ```
   cd {dist.dev}
   svn add {version}-rc{rc}
   svn commit -m “Apache Ignite {version} RC{rc}”
   ```
14. Put the release on a vote on the developers mailing list.

## Finalizing the Release

Perform the following actions ONLY after the vote is successful and closed.

1. Login to the Apache Nexus and release the staging repository: https://repository.apache.org/#stagingRepositories
2. Create an empty folder under the release distribution directory:
   ```
   rm -rf {dist.release}/{version}
   mkdir {dist.release}/{version}
   ```
3. Copy ZIP packages along with checksums and signatures to the release distribution directory:
   ```
   cp {dist.dev}/{version}-rc{rc}/* {dist.release}/{version}
   ```
4. Commit ZIP packages:
   ```
   cd {dist.release}
   svn add {version}
   svn commit -m “Apache Ignite {version}”
   ```
   