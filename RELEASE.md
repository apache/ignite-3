# Apache Ignite 3 Release Procedure

This document describes the current procedure for preparing an Ignite 3 release.

## Requirements

1. **Docker 19.03+**
   * Verify docker installation: `docker version`
   * Verify docker buildx installation: `docker buildx version`
2. **.NET SDK 8.0.407+**
   * Verify .NET installation: `dotnet --version`
3. * Also some specific packages like: `doxygen`, no need to list them all, it`s easy enough to understand it from possible output errors.

## Prerequisites

1. Create a GPG key, upload it to a keyserver, and locate its ID. More details here: https://infra.apache.org/openpgp.html
2. Checkout Apache distribution directories:
   ```
   svn checkout https://dist.apache.org/repos/dist/dev/ignite dist-dev
   svn checkout https://dist.apache.org/repos/dist/release/ignite dist-release
   ```
3. Provide your Apache credentials to Gradle (required for uploading to Apache Nexus) - create or update `~/.gradle/gradle.properties` file with the following content:
   ```
   staging_user=*INSERT STAGING USERNAME HERE*
   staging_password=*INSERT STAGING PASSWORD HERE*
   ```
   (do NOT put secrets into `gradle.properties` in the project dir - see [Gradle docs](https://docs.gradle.org/current/userguide/build_environment.html) for more details).

For all the commands going forward:
* Replace `{version}` with the version number being released.
* Replace `{rc}` with the current sequential number of the release candidate.
* Replace `{gpg}` with your GPG key ID.
* Replace `{dist.dev}` with the local path to the development distribution directory.
* Replace `{dist.release}` with the local path to the release distribution directory.

4. Collect all release important unresolved issues and label them with smth. like: `release-{version}-blocker`, inform community that all further critical release issues need to be labeled appropriate. It is not necessary, but it will allow not to miss important issues.
5. Check license criteria for [all dependencies](https://www.apache.org/legal/resolved.html) run `./gradlew :packaging:generateLicenseReport` and check report `./packaging/build/reports/dependency-license`.

## Preparing the Release

1. Go to the project home folder.
2. Update RELEASE_NOTES.txt with the changes since the last release. Commit and push to the `main` branch.
3. Update versions in `main` branch to the next development version (e.g., `x.y.z-SNAPSHOT`):
   * Update `gradle.properties` manually.
   * Run `./gradlew :platforms:updateVersion` to update platforms versions (.NET, C++, Python, etc.)
   * Commit and push changes.
4. Create and push a new branch for the release `ignite-{version}`.
5. Update versions in `ignite-{version}` branch to the current release version, remove `-SNAPSHOT` suffix - same steps as in point 3.
6. Create a Git tag from `ignite-{version}` branch head:
   ```
   git tag -a {version}-rc{rc} -m "{version}-rc{rc}"
   git push --tags
   ```
7. Setup Gradle properties - create or update `~/.gradle/gradle.properties` file with the following content:
   ```
   signing.keyId=*INSERT KEY HERE LAST 8 CHARS*
   signing.password=*INSERT PASSWORD HERE*
   signing.secretKeyRingFile=*INSERT KEY RING ABSOLUTE PATH HERE*
   ```
   (do NOT put secrets into `gradle.properties` in the project dir - see [Gradle docs](https://docs.gradle.org/current/userguide/build_environment.html) for more details).
   To generate a secret key ring file use the following command
   ```
   gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg
   ```
   Show key id command (you need only last 8 chars from printed key)
   ```
   gpg -K
   ```
8. Build the project, sign the artifact and create a staging repository:
   ```
   ./gradlew publishAllPublicationsToMavenRepository
   ```
9. Login to the Apache Nexus and close the new repository: https://repository.apache.org/#stagingRepositories
10. Create an empty folder under the development distribution directory:
   ```
   rm -rf {dist.dev}/{version}-rc{rc}
   mkdir {dist.dev}/{version}-rc{rc}
   ```
11. Create ZIP, DEB, RPM packages, .NET, Java and C++ client, sign them and create checksums:
   ```
   ./gradlew -PprepareRelease prepareRelease -Pplatforms.enable
   ```
12. Create Docker Images:
    ```
    ./gradlew :packaging:docker -Ptarget_platform=linux/amd64 -Pplatforms.enable
    docker save apacheignite/ignite:{version} -o packaging/build/release/ignite:{version}-amd64.tar
    ./gradlew :packaging:docker -Ptarget_platform=linux/arm64 -Pplatforms.enable
    docker save apacheignite/ignite:{version} -o packaging/build/release/ignite:{version}-arm64.tar
    ```
    Probably you will need some steps from [multi-platform](https://docs.docker.com/build/building/multi-platform) doc.
    Minimal steps during first release:
    ```
    docker buildx build --load -t linux/arm64 --builder=container .
    docker run --privileged --rm tonistiigi/binfmt --install all
    ```
13. ODBC: run appropriate TC suites:
    ```
    https://ci.ignite.apache.org/buildConfiguration/ApacheIgnite3xGradle_Release_Build_OdbcRpmPackage
    https://ci.ignite.apache.org/buildConfiguration/ApacheIgnite3xGradle_Release_Build_7odbcDebPackage
    ```
    download artifacts, remove zip and sign all files with gpg key. After - copy into `{dist.dev}/{version}-rc{rc}`
14. Copy all packages along with checksums and signatures to the development distribution directory:
   ```
   cp packaging/build/release/* {dist.dev}/{version}-rc{rc}
   ```
15. Check for ignite.version.full (TODO [IGNITE-IGNITE-26834](https://issues.apache.org/jira/browse/IGNITE-26834)), remove this step if it will be done automatically through gradle task.
16. Commit ZIP and DEB\RPM packages:
   ```
   cd {dist.dev}
   svn add {version}-rc{rc}
   svn commit -m “Apache Ignite {version} RC{rc}”
   ``` 
17. Put the release on a vote on the developers mailing list.
18. If vote is passed - send appropriate message for dev-list, otherwise apply appropriate changes, don\`t forget to move `git tag` if there are code related changes.

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
5. Build and publish the documentation - see docs/README.adoc
   * Push to https://github.com/apache/ignite-website/tree/master/docs/ignite3 
6. Build and publish API docs
   * Java: `./gradlew aggregateJavadoc`
   * .NET: `./gradlew :platforms:aggregateDotnetDocs`
   * C++: `./gradlew :platforms:doxygenCppClient`
   * Push to https://github.com/apache/ignite-website/tree/master/releases
7. Publish Docker images:
   * `./gradlew :packaging:docker -Ptarget_platform=linux/amd64,linux/arm64 -Pdocker_push -Pplatforms.enable`
     (PMC only) otherwise you need to request credentials from PMC-only area in SVN
8. Publish NuGet packages:
   * Get API key from https://svn.apache.org/repos/private/pmc/ignite/credentials/nuget.org (PMC only)
   * `for i in *.nupkg; do dotnet nuget push $i -k API_KEY_HERE -s "https://nuget.org/"; done`
   (PMC only) otherwise you need to request credentials from PMC-only area in SVN
9. Publish Python packages (TODO [IGNITE-24327](https://issues.apache.org/jira/browse/IGNITE-24327))
10. Update https://ignite.apache.org/download.cgi page - see https://cwiki.apache.org/confluence/display/IGNITE/Website+Development, https://cwiki.apache.org/confluence/display/IGNITE/How+to+Document
11. https://github.com/apache/ignite-website/blob/master/docs/ignite3/available-versions.txt new version need to be first
12. https://github.com/apache/ignite-website/blob/master/docs/ignite3/latest need to contain new version
13. Update [download links](https://ignite.apache.org/download.cgi) `_src/_components/download-binary.pug` and `_src/_components/download-source.pug` (and others if any were added). Copy the +downloadRow section and edit appropriately.
14. Write a [blog post](https://ignite.apache.org/blog/) announcing the release and listing the major changes and improvements. Add a new `.pug` file in `_src/_blog`.

## Post Release steps

1. Add compatibility snapshot, OpenAPI spec and update `igniteVersions.json` for released version:

   * On release git branch run the following command to generate new snapshot with compatibility information, copy OpenAPI spec and update the versions file `./gradlew postRelease`
   * Switch git to main branch
   * Run compatibility test `./gradlew :ignite-runner:test --tests "org.apache.ignite.internal.configuration.compatibility.ConfigurationCompatibilityTest"`
   * Add new files to git. Commit them under separate JIRA ticket with commit message `{jira_ticket} Add compatibility data for Ignite {version}`. Push the change to main branch.
