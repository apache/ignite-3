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

import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_REQUEST;
import static org.apache.ignite.internal.rest.constants.HttpCode.CONFLICT;
import static org.apache.ignite.internal.rest.constants.HttpCode.NOT_FOUND;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.createZipFile;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.fillDummyFile;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.multipart.MultipartBody;
import io.micronaut.http.client.multipart.MultipartBody.Builder;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.deployment.UnitStatus;
import org.apache.ignite.internal.rest.api.deployment.UnitVersionStatus;
import org.apache.ignite.internal.rest.matcher.ProblemMatcher;
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
            createZipFile(Map.of(
                    "a1/a2", SIZE_IN_BYTES,
                    "b1", SIZE_IN_BYTES,
                    "c1/c2/c3/c4", BIG_IN_BYTES,
                    "d1/d2", SIZE_IN_BYTES,
                    "d1/a2", SIZE_IN_BYTES
            ), zipFile);
        }
    }

    @AfterEach
    public void cleanup() {
        List<UnitStatus> list = list(UNIT_ID);

        for (UnitStatus unitStatus : list) {
            for (UnitVersionStatus versionToStatus : unitStatus.versionToStatus()) {
                if (versionToStatus.getStatus() == DEPLOYED) {
                    HttpResponse<Object> response = undeploy(UNIT_ID, versionToStatus.getVersion());
                    assertThat(response.code(), is(OK.code()));
                }
            }
        }

        await().untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            Collection<UnitStatus> statuses = client.toBlocking().retrieve(get, Argument.listOf(UnitStatus.class));

            assertThat(statuses, is(empty()));
        });

    }

    @Test
    public void testDeploySuccessful() {
        String id = UNIT_ID;
        String version = "1.1.1";
        HttpResponse<Object> response = deploy(id, version);

        assertThat(response.code(), is(OK.code()));

        await().untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            UnitStatus status = client.toBlocking().retrieve(get, UnitStatus.class);

            assertThat(status.id(), is(id));
            assertThat(status.versionToStatus(), equalTo(List.of(new UnitVersionStatus(version, DEPLOYED))));
        });
    }

    @Test
    public void testDeployBig() {
        String id = UNIT_ID;
        String version = "1.1.1";
        HttpResponse<Object> response = deploy(id, version, false, bigFile);

        assertThat(response.code(), is(OK.code()));
    }

    @Test
    public void testDeployFailedWithoutContent() {
        String id = "unitId";
        String version = "1.1.1";
        HttpClientResponseException e = assertThrows(
                HttpClientResponseException.class,
                () -> deploy(id, version, false, null));
        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));
    }

    @Test
    public void testDeployExisted() {
        String id = UNIT_ID;
        String version = "1.1.1";
        HttpResponse<Object> response = deploy(id, version);

        assertThat(response.code(), is(OK.code()));

        HttpClientResponseException e = assertThrows(
                HttpClientResponseException.class,
                () -> deploy(id, version));
        assertThat(e.getResponse().code(), is(CONFLICT.code()));
    }

    @Test
    public void testDeployUndeploy() {
        String id = UNIT_ID;
        String version = "1.1.1";

        HttpResponse<Object> response = deploy(id, version);

        assertThat(response.code(), is(OK.code()));

        response = undeploy(id, version);
        assertThat(response.code(), is(OK.code()));
    }

    @Test
    public void testUndeployFailed() {
        HttpClientResponseException e = assertThrows(
                HttpClientResponseException.class,
                () -> undeploy(UNIT_ID, "1.1.1"));
        assertThat(e.getResponse().code(), is(NOT_FOUND.code()));
    }

    @Test
    public void testEmpty() {
        String id = "nonExisted";

        assertThat(list(id), is(empty()));
    }

    @Test
    public void testList() {
        String id = UNIT_ID;
        deploy(id, "1.1.1");
        deploy(id, "1.1.2");
        deploy(id, "1.2.1");
        deploy(id, "2.0");
        deploy(id, "1.0.0");
        deploy(id, "1.0.1");

        List<UnitStatus> list = list(id);

        List<String> versions = list.stream()
                .flatMap(unitStatus -> unitStatus.versionToStatus().stream().map(UnitVersionStatus::getVersion))
                .collect(Collectors.toList());
        assertThat(versions, contains("1.0.0", "1.0.1", "1.1.1", "1.1.2", "1.2.1", "2.0.0"));
    }

    @Test
    public void testZipDeploy() {
        String id = UNIT_ID;
        String version = "1.1.1";
        HttpResponse<Object> response = deployZip(id, version);

        assertThat(response.code(), is(OK.code()));

        await().untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            UnitStatus status = client.toBlocking().retrieve(get, UnitStatus.class);

            assertThat(status.id(), is(id));
            assertThat(status.versionToStatus(), equalTo(List.of(new UnitVersionStatus(version, DEPLOYED))));
        });

        Path workDir0 = CLUSTER.nodeWorkDir(0);
        Path nodeUnitDirectory = workDir0.resolve("deployment").resolve(id).resolve(version);

        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry ze;
            while ((ze = zis.getNextEntry()) != null) {
                assertTrue(Files.exists(nodeUnitDirectory.resolve(ze.getName())), "File " + ze.getName() + " does not exist");
            }
        } catch (IOException e) {
            fail(e);
        }
    }

    @Test
    public void testZipDeployAsFile() {
        String id = UNIT_ID;
        String version = "1.1.1";
        HttpResponse<Object> response = deploy(id, version, false, zipFile);

        assertThat(response.code(), is(OK.code()));

        await().untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            UnitStatus status = client.toBlocking().retrieve(get, UnitStatus.class);

            assertThat(status.id(), is(id));
            assertThat(status.versionToStatus(), equalTo(List.of(new UnitVersionStatus(version, DEPLOYED))));
        });

        Path workDir0 = CLUSTER.nodeWorkDir(0);
        Path nodeUnitDirectory = workDir0.resolve("deployment").resolve(id).resolve(version);

        assertTrue(Files.exists(nodeUnitDirectory.resolve(zipFile.getFileName())));
    }

    @Test
    public void testDeployFileAsZip() {
        String id = UNIT_ID;
        String version = "1.1.1";

        assertThrowsProblem(
                () -> deploy(id, version, true, smallFile),
                HttpStatus.BAD_REQUEST,
                ProblemMatcher.isProblem().withDetail("Only zip file is supported.")
        );

        assertThrowsProblem(
                () -> deploy(id, version, true, zipFile, zipFile),
                HttpStatus.BAD_REQUEST,
                ProblemMatcher.isProblem().withDetail("Deployment unit with unzip supports only single zip file.")
        );
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
}
