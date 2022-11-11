# Apache Ignite 3 Release Procedure

This document describes the current procedure for preparing an Ignite 3 release.

## Prerequisites

1. Create a GPG key, upload it to a keyserver, and locate its ID. More details here: https://infra.apache.org/openpgp.html
2. Checkout Apache distribution directories:
   ```
   svn checkout https://dist.apache.org/repos/dist/dev/ignite dist-dev
   svn checkout https://dist.apache.org/repos/dist/release/ignite dist-release
   ```
3. Provide your Apache credentials to Gradle (required for uploading to Apache Nexus):
   ```
   staging_user=*INSERT STAGING USERNAME HERE*
   staging_password=*INSERT STAGING PASSWORD HERE*
   ```
   You can specify it in project gradle.property but DO NOT FORGET revert it before push.
   Better place is gradle.properties in HOME dir, read about it https://docs.gradle.org/current/userguide/build_environment.html

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
3. Setup properties in gradle.properties  
   You can specify it in project gradle.property but DO NOT FORGET revert it before push.
   Better place is gradle.properties in HOME dir, read about it https://docs.gradle.org/current/userguide/build_environment.html
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
4. Build the project, sign the artifact and create a staging repository:
   ```
   ./gradlew publishAllPublicationsToMavenRepository
   ```
5. Login to the Apache Nexus and close the new repository: https://repository.apache.org/#stagingRepositories
6. Create an empty folder under the development distribution directory:
   ```
   rm -rf {dist.dev}/{version}-rc{rc}
   mkdir {dist.dev}/{version}-rc{rc}
   ```
7. Create a signed src zip:
   ```
   ./gradlew buildAndSignAllSrcZip
   ```
8. Copy src zip with checksums and signatures
   ```
   cp packaging/build/distributions/apache-ignite-3.0.0-beta1-src.zip \
   packaging/build/distributions/ignite3-{version}.zip.asc \ 
   packaging/build/distributions/ignite3-{version}.zip.sha512 \
   {dist.dev}/{version}-rc{rc}
   ```
9. Create zip distributions 
   ```
   ./gradlew allDistZip
   ```
10. Sign zip distributions
    ```
    ./gradlew signAllDistZip signCliZip signDbZip
    ```
11. Copy ZIP packages along with checksums and signatures to the development distribution directory:
    ```
    cp packaging/build/distributions/ignite3-{version}.zip \
    packaging/build/distributions/ignite3-{version}.zip.asc \
    packaging/build/distributions/ignite3-{version}.zip.sha512 \
    packaging/build/db/distributions/ignite3db-{version}.zip \
    packaging/build/db/distributions/ignite3db-{version}.zip.asc \
    packaging/build/db/distributions/ignite3db-{version}.zip.sha512 \
    packaging/build/cli/distributions/ignite3cli-{version}.zip \
    packaging/build/cli/distributions/ignite3cli-{version}.zip.asc \
    packaging/build/cli/distributions/ignite3cli-{version}.zip.sha512 \
    /Users/vkulichenko/GridGain/dist-dev/{version}-rc{rc}
    ```
12. Create DEB\RPM distributions, they will be signed 
   ```
   ./gradlew buildDeb buildRpm
   ```
13. Copy DEB\RPM packages to the development distribution directory:
   ```
   cp packaging/db/build/distributions/*.deb packaging/db/build/distributions/*.changes \
   packaging/db/build/distributions/*.rpm \
   packaging/cli/build/distributions/*.deb packaging/cli/build/distributions/*.changes \
   packaging/cli/build/distributions/*.rpm \
   {dist.dev}/{version}-rc{rc}
   ```
14. Create Dotnet and C++ clients 
   ```
   ./gradlew buildAndSignCppClient buildAndSignNuGetZip
   ```
15. Copy zip with checksums and signatures
   ```
   cp modules/platforms/build/distributions/apache-ignite-3.0.0-beta1-cpp.zip \
   modules/platforms/build/distributions/apache-ignite-3.0.0-beta1-cpp.zip.asc \ 
   modules/platforms/build/distributions/apache-ignite-3.0.0-beta1-cpp.zip.sha512 \  
   modules/platforms/build/distributions/apache-ignite-3.0.0-beta1-nuget.zip \
   modules/platforms/build/distributions/apache-ignite-3.0.0-beta1-nuget.zip.asc \ 
   modules/platforms/build/distributions/apache-ignite-3.0.0-beta1-nuget.zip.sha512 \ 
   {dist.dev}/{version}-rc{rc}
   ```
16. Commit ZIP and DEB\RPM packages:
   ```
   cd {dist.dev}
   svn add {version}-rc{rc}
   svn commit -m “Apache Ignite {version} RC{rc}”
   ```
17. Put the release on a vote on the developers mailing list.

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
   