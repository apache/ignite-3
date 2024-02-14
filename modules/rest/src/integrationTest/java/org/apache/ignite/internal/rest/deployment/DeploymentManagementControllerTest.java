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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_REQUEST;
import static org.apache.ignite.internal.rest.constants.HttpCode.CONFLICT;
import static org.apache.ignite.internal.rest.constants.HttpCode.NOT_FOUND;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.multipart.MultipartBody;
import io.micronaut.http.client.multipart.MultipartBody.Builder;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.rest.api.deployment.UnitStatus;
import org.apache.ignite.internal.rest.api.deployment.UnitVersionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for REST controller {@link DeploymentManagementController}.
 */
@MicronautTest(rebuildContext = true)
public class DeploymentManagementControllerTest extends ClusterPerTestIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + Cluster.BASE_HTTP_PORT;

    private Path dummyFile;

    private static final long SIZE_IN_BYTES = 1024L;

    @Inject
    @Client(NODE_URL + "/management/v1/deployment")
    HttpClient client;

    @BeforeEach
    public void setup() throws IOException {
        dummyFile = workDir.resolve("dummy.txt");

        if (!Files.exists(dummyFile)) {
            try (SeekableByteChannel channel = Files.newByteChannel(dummyFile, WRITE, CREATE)) {
                channel.position(SIZE_IN_BYTES - 4);

                ByteBuffer buf = ByteBuffer.allocate(4).putInt(2);
                buf.rewind();
                channel.write(buf);
            }
        }
    }

    @Test
    public void testDeploySuccessful() {
        String id = "testId";
        String version = "1.1.1";
        HttpResponse<Object> response = deploy(id, version);

        assertThat(response.code(), is(OK.code()));

        await().timeout(10, SECONDS).untilAsserted(() -> {
            MutableHttpRequest<Object> get = HttpRequest.GET("cluster/units");
            UnitStatus status = client.toBlocking().retrieve(get, UnitStatus.class);

            assertThat(status.id(), is(id));
            assertThat(status.versionToStatus(), equalTo(List.of(new UnitVersionStatus(version, DEPLOYED))));
        });
    }

    @Test
    public void testDeployFailedWithoutContent() {
        String id = "unitId";
        String version = "1.1.1";
        HttpClientResponseException e = assertThrows(
                HttpClientResponseException.class,
                () -> deploy(id, version, null));
        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));
    }

    @Test
    public void testDeployExisted() {
        String id = "testId";
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
        String id = "testId";
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
                () -> undeploy("testId", "1.1.1"));
        assertThat(e.getResponse().code(), is(NOT_FOUND.code()));
    }

    @Test
    public void testEmpty() {
        String id = "nonExisted";

        assertThat(list(id), is(empty()));
    }

    @Test
    public void testList() {
        String id = "unitId";
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

    private HttpResponse<Object> deploy(String id, String version) {
        return deploy(id, version, dummyFile.toFile());
    }

    private HttpResponse<Object> deploy(String id, String version, File file) {
        MultipartBody body = null;

        if (file != null) {
            Builder builder = MultipartBody.builder();
            builder.addPart("unitContent", file);
            body = builder.build();
        }

        MutableHttpRequest<MultipartBody> post = HttpRequest
                .POST("units/" + id + "/" + version, body)
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
