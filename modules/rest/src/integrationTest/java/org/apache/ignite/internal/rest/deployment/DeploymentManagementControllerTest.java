/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.deployment;

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static io.micronaut.http.HttpStatus.CONFLICT;
import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static io.micronaut.http.HttpStatus.OK;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.createZipFile;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.fillDummyFile;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.io.FileMatchers.aFileWithSize;
import static org.hamcrest.io.FileMatchers.anExistingFile;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.multipart.MultipartBody;
import io.micronaut.http.client.multipart.MultipartBody.Builder;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry.UnitFile;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry.UnitFolder;
import org.apache.ignite.internal.rest.api.deployment.UnitStatus;
import org.apache.ignite.internal.rest.api.deployment.UnitVersionStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for REST controller {@link DeploymentManagementController}.
 */
@MicronautTest(rebuildContext = true)
public class DeploymentManagementControllerTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private Path smallFile;

    private Path bigFile;

    private Path zipFile;

    private static final long SIZE_IN_BYTES = 1024L;

    private static final long BIG_IN_BYTES = 100 * 1024L * 1024L;  // 100 MiB

    private static final String UNIT_ID = "unitId";
    private static final String VERSION = "1.1.1";

    @Inject
    @Client(NODE_URL + "/management/v1/deployment")
    HttpClient client;

    @BeforeEach
    public void setup() throws IOException {
        smallFile = WORK_DIR.resolve("small.txt");
        bigFile = WORK_DIR.resolve("big.txt");
        zipFile = WORK_DIR.resolve("zip.zip");

        if (!Files.exists(smallFile)) {
            fillDummyFile(smallFile, SIZE_IN_BYTES);
        }
        if (!Files.exists(bigFile)) {
            fillDummyFile(bigFile, BIG_IN_BYTES);
        }
        if (!Files.exists(zipFile)) {
            // Assign identical filenames to the files in different directories to test duplicate filenames detection
            createZipFile(Map.of(
                    "file1", SIZE_IN_BYTES,
                    "dir1/file", SIZE_IN_BYTES,
                    "dir2/dir/dir/file", BIG_IN_BYTES,
                    "dir3/file1", SIZE_IN_BYTES,
                    "dir3/file2", SIZE_IN_BYTES
            ), zipFile);
        }
    }

    @AfterEach
    public void cleanup() {
        IgniteDeployment deployment = igniteImpl(0).deployment();
        CompletableFuture<List<UnitStatuses>> clusterStatusesFut = deployment.clusterStatusesAsync();
        assertThat(clusterStatusesFut, willCompleteSuccessfully());
        List<UnitStatuses> clusterStatuses = clusterStatusesFut.join();

        for (UnitStatuses unitStatuses : clusterStatuses) {
            for (org.apache.ignite.internal.deployunit.UnitVersionStatus unitVersionStatus : unitStatuses.versionStatuses()) {
                if (unitVersionStatus.getStatus() == DeploymentStatus.DEPLOYED) {
                    assertThat(deployment.undeployAsync(UNIT_ID, unitVersionStatus.getVersion()), willCompleteSuccessfully());
                }
            }
        }

        await().pollDelay(Duration.ZERO).until(deployment::clusterStatusesAsync, willBe(empty()));
    }

    @Test
    public void testDeploySuccessful() {
        assertThat(deploy(UNIT_ID, VERSION), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, VERSION);
    }

    @Test
    public void testDeployBig() {
        assertThat(deploy(UNIT_ID, VERSION, false, bigFile), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, VERSION);
    }

    @Test
    public void versions() throws IOException {
        // Pass multiple files to test discarding in the error handler in the controller.
        Path[] files = IntStream.range(0, 5).mapToObj(i -> WORK_DIR.resolve("file" + i + ".txt")).toArray(Path[]::new);
        for (Path file : files) {
            fillDummyFile(file, 4);
        }

        assertThrowsProblem(
                () -> deploy(UNIT_ID, "1.1.1.1-foo_", false, files),
                isProblem().withStatus(BAD_REQUEST).withDetail("Invalid version format of provided version: 1.1.1.1-foo_")
        );

        String version = "1.1.1.1-foo";

        assertThat(deploy(UNIT_ID, version, false, smallFile), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, version);

        assertThat(list(UNIT_ID), contains(new UnitStatus(UNIT_ID, List.of(new UnitVersionStatus(version, DEPLOYED)))));

        assertThat(undeploy(UNIT_ID, version), hasStatus(OK));
    }

    @Test
    public void testDeployFailedWithoutContent() {
        assertThrowsProblem(
                () -> deploy(UNIT_ID, VERSION, false, null),
                isProblem().withStatus(BAD_REQUEST)
        );
    }

    @Test
    public void testDeployFailedWithDuplicateFilenames() {
        assertThrowsProblem(
                () -> deploy(UNIT_ID, VERSION, false, smallFile, smallFile),
                isProblem().withStatus(BAD_REQUEST).withDetail("Duplicate filename: small.txt")
        );
    }

    @Test
    public void testDeployFailedWithCaseInsensitiveDuplicateFilenames() throws IOException {
        // Create files with names that differ only in case.
        Path file1 = createDummyFileInDir("case_test_1", "TestFile.txt");
        Path file2 = createDummyFileInDir("case_test_2", "testfile.txt");

        if (isCaseInsensitiveFileSystem()) {
            assertThrowsProblem(
                    () -> deploy(UNIT_ID, VERSION, false, file1, file2),
                    isProblem().withStatus(BAD_REQUEST).withDetail(containsString("Duplicate filename: testfile.txt"))
            );
        } else {
            assertThat(deploy(UNIT_ID, VERSION, false, file1, file2), hasStatus(OK));
        }
    }

    @Test
    public void testZipDeployFailedWithCaseInsensitiveDuplicates() throws IOException {
        Path zipFileWithDuplicates = WORK_DIR.resolve("zipWithDuplicates.zip");

        createZipWithCaseVariantEntries(zipFileWithDuplicates);

        if (isCaseInsensitiveFileSystem()) {
            assertThrowsProblem(
                    () -> deploy(UNIT_ID, VERSION, true, zipFileWithDuplicates),
                    isProblem().withStatus(BAD_REQUEST).withDetail(containsString("ZIP contains case-insensitive duplicate: testfile.txt"))
            );
        } else {
            assertThat(deploy(UNIT_ID, VERSION, true, zipFileWithDuplicates), hasStatus(OK));
        }
    }

    private static Path createDummyFileInDir(String dirName, String fileName) throws IOException {
        Path file = WORK_DIR.resolve(dirName).resolve(fileName);
        Files.createDirectories(file.getParent());
        fillDummyFile(file, SIZE_IN_BYTES);
        return file;
    }

    private static void createZipWithCaseVariantEntries(Path zipPath) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {
            // Add first entry
            ZipEntry entry1 = new ZipEntry("TestFile.txt");
            zos.putNextEntry(entry1);
            zos.write("content1".getBytes());
            zos.closeEntry();

            // Add second entry with same name but different case
            ZipEntry entry2 = new ZipEntry("testfile.txt");
            zos.putNextEntry(entry2);
            zos.write("content2".getBytes());
            zos.closeEntry();
        }
    }

    private static boolean isCaseInsensitiveFileSystem() throws IOException {
        // Check if filesystem is case-insensitive by testing if we can detect the case variation
        Path probeFile = WORK_DIR.resolve("CaseSensitivityProbe");
        Files.deleteIfExists(probeFile);
        Files.createFile(probeFile);
        try {
            return Files.exists(WORK_DIR.resolve("casesensitivityprobe"));
        } finally {
            Files.deleteIfExists(probeFile);
        }
    }

    @Test
    public void testDeployExisted() {
        assertThat(deploy(UNIT_ID, VERSION), hasStatus(OK));

        assertThrowsProblem(
                () -> deploy(UNIT_ID, VERSION),
                isProblem().withStatus(CONFLICT)
        );

        awaitDeployedStatus(UNIT_ID, VERSION);
    }

    @Test
    public void testDeployUndeploy() {
        assertThat(deploy(UNIT_ID, VERSION), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, VERSION);

        assertThat(undeploy(UNIT_ID, VERSION), hasStatus(OK));
    }

    @Test
    public void testUndeployFailed() {
        assertThrowsProblem(
                () -> undeploy(UNIT_ID, VERSION),
                isProblem().withStatus(NOT_FOUND)
        );
    }

    @Test
    public void testEmpty() {
        String id = "nonExisted";

        assertThat(list(id), is(empty()));
    }

    @Test
    public void testList() {
        String[] versions = {"1.1.1", "1.1.2", "1.2.1", "2.0.0", "1.0.0", "1.0.1" };
        for (String version : versions) {
            deploy(UNIT_ID, version);
        }

        awaitDeployedStatus(UNIT_ID, versions);

        List<UnitStatus> list = list(UNIT_ID);

        List<String> actualVersions = list.stream()
                .flatMap(unitStatus -> unitStatus.versionToStatus().stream().map(UnitVersionStatus::getVersion))
                .collect(Collectors.toList());
        assertThat(actualVersions, containsInAnyOrder(versions));
    }

    @Test
    public void testZipDeploy() throws IOException {
        assertThat(deployZip(UNIT_ID, VERSION), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, VERSION);

        Path workDir0 = CLUSTER.nodeWorkDir(0);
        Path nodeUnitDirectory = workDir0.resolve("deployment").resolve(UNIT_ID).resolve(VERSION);

        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry ze;
            while ((ze = zis.getNextEntry()) != null) {
                assertThat(nodeUnitDirectory.resolve(ze.getName()).toFile(), anExistingFile());
            }
        }
    }

    @Test
    public void testZipDeployAsFile() {
        assertThat(deploy(UNIT_ID, VERSION, false, zipFile), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, VERSION);

        await().untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            UnitStatus status = client.toBlocking().retrieve(get, UnitStatus.class);

            assertThat(status.id(), is(UNIT_ID));
            assertThat(status.versionToStatus(), equalTo(List.of(new UnitVersionStatus(VERSION, DEPLOYED))));
        });

        Path workDir0 = CLUSTER.nodeWorkDir(0);
        Path nodeUnitDirectory = workDir0.resolve("deployment").resolve(UNIT_ID).resolve(VERSION);

        assertThat(nodeUnitDirectory.resolve(zipFile.getFileName()).toFile(), anExistingFile());
    }

    @Test
    public void testDeployFileAsZip() {
        assertThrowsProblem(
                () -> deploy(UNIT_ID, VERSION, true, smallFile),
                isProblem().withStatus(BAD_REQUEST).withDetail("Only zip file is supported.")
        );

        assertThrowsProblem(
                () -> deploy(UNIT_ID, VERSION, true, zipFile, zipFile),
                isProblem().withStatus(BAD_REQUEST).withDetail("Deployment unit with unzip supports only single zip file.")
        );
    }

    @Test
    public void testUnitContent() {
        assertThat(deploy(UNIT_ID, VERSION, false, smallFile, zipFile), hasStatus(OK));

        awaitDeployedStatus(UNIT_ID, VERSION);

        UnitFolder folder = folder(UNIT_ID, VERSION);

        Path workDir0 = CLUSTER.nodeWorkDir(0);
        Path nodeUnitDirectory = workDir0.resolve("deployment").resolve(UNIT_ID).resolve(VERSION);

        for (UnitEntry child : folder.children()) {
            verifyEntry(child, nodeUnitDirectory);
        }
    }

    private static void verifyEntry(UnitEntry entry, Path currentDir) {
        if (entry instanceof UnitFile) {
            UnitFile file = (UnitFile) entry;
            assertThat(currentDir.resolve(file.name()).toFile(), aFileWithSize(file.size()));
        } else if (entry instanceof UnitFolder) {
            Path dir = currentDir.resolve(entry.name());
            for (UnitEntry child : ((UnitFolder) entry).children()) {
                verifyEntry(child, dir);
            }
        } else {
            throw new IllegalStateException("Unit entry type not supported.");
        }
    }

    private void awaitDeployedStatus(String id, String... versions) {
        await().untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            UnitStatus status = client.toBlocking().retrieve(get, UnitStatus.class);

            assertThat(status.id(), is(id));
            UnitVersionStatus[] statuses = Arrays.stream(versions)
                    .map(version -> new UnitVersionStatus(version, DEPLOYED))
                    .toArray(UnitVersionStatus[]::new);

            assertThat(status.versionToStatus(), containsInAnyOrder(statuses));
        });
    }

    private HttpResponse<Object> deployZip(String id, String version) {
        return deploy(id, version, true, zipFile);
    }

    private HttpResponse<Object> deploy(String id, String version) {
        return deploy(id, version, false, smallFile);
    }

    private HttpResponse<Object> deploy(String id, String version, boolean unzip, Path... files) {
        MultipartBody body = null;

        if (files != null) {
            Builder builder = MultipartBody.builder();
            for (Path file : files) {
                builder.addPart("unitContent", file.toFile());
            }
            body = builder.build();
        }

        MutableHttpRequest<MultipartBody> post = HttpRequest
                .POST("units/" + (unzip ? "zip/" : "") + id + "/" + version, body)
                .contentType(MediaType.MULTIPART_FORM_DATA);

        return client.toBlocking().exchange(post);
    }

    private HttpResponse<Object> undeploy(String id, String version) {
        MutableHttpRequest<Object> delete = HttpRequest
                .DELETE("units/" + id + "/" + version)
                .contentType(MediaType.APPLICATION_JSON);

        return client.toBlocking().exchange(delete);
    }

    private List<UnitStatus> list(String id) {
        MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units/" + id);

        return client.toBlocking().retrieve(get, Argument.listOf(UnitStatus.class));
    }

    private UnitFolder folder(String id, String version) {
        MutableHttpRequest<Object> get = HttpRequest.GET("node/units/structure/" + id + "/" + version);

        return client.toBlocking().retrieve(get, UnitFolder.class);
    }
}
