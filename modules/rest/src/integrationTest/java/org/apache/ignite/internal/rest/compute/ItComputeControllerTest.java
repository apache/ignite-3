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

package org.apache.ignite.internal.rest.compute;

import static io.micronaut.http.HttpRequest.DELETE;
import static io.micronaut.http.HttpRequest.PUT;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.internal.rest.matcher.RestJobStatusMatcher.canceled;
import static org.apache.ignite.internal.rest.matcher.RestJobStatusMatcher.completed;
import static org.apache.ignite.internal.rest.matcher.RestJobStatusMatcher.executing;
import static org.apache.ignite.internal.rest.matcher.RestJobStatusMatcher.queued;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.api.compute.JobStatus;
import org.apache.ignite.internal.rest.api.compute.UpdateJobPriorityBody;
import org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link ComputeController}.
 */
@MicronautTest
public class ItComputeControllerTest extends ClusterPerClassIntegrationTest {
    private static final String COMPUTE_URL = "/management/v1/compute/";

    private static final Object LOCK = new Object();

    @Inject
    @Client("http://localhost:10300" + COMPUTE_URL)
    HttpClient client0;

    @Inject
    @Client("http://localhost:10301" + COMPUTE_URL)
    HttpClient client1;

    @Inject
    @Client("http://localhost:10303" + COMPUTE_URL)
    HttpClient client2;

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "{\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector: { port:{} },\n"
                + "  rest.port: {},\n"
                + "  compute.threadPoolSize: 1 \n"
                + "}";
    }

    @AfterEach
    void tearDown() {
        // Cancel all jobs.
        getJobStatuses(client0).values().stream()
                .filter(it -> it.finishTime() == null)
                .map(JobStatus::id)
                .forEach(jobId -> cancelJob(client0, jobId));

        // Wait for all jobs to complete.
        await().until(() -> {
            Collection<JobStatus> statuses = getJobStatuses(client0).values();

            for (JobStatus status : statuses) {
                if (status.finishTime() == null) {
                    return false;
                }
            }

            return true;
        });
    }

    @Test
    void shouldReturnStatusesOfAllJobs() {
        IgniteImpl entryNode = CLUSTER.node(0);

        JobExecution<String> localExecution = runBlockingJob(entryNode, Set.of(entryNode.node()));

        JobExecution<String> remoteExecution = runBlockingJob(entryNode, Set.of(CLUSTER.node(1).node()));

        UUID localJobId = localExecution.idAsync().join();
        UUID remoteJobId = remoteExecution.idAsync().join();

        await().untilAsserted(() -> {
            Map<UUID, JobStatus> statuses = getJobStatuses(client0);

            assertThat(statuses.get(localJobId), executing(localJobId));
            assertThat(statuses.get(remoteJobId), executing(remoteJobId));
        });
    }

    @Test
    void shouldReturnStatusOfLocalJob() {
        IgniteImpl entryNode = CLUSTER.node(0);

        JobExecution<String> execution = runBlockingJob(entryNode, Set.of(entryNode.node()));

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        unblockJob();

        await().until(() -> getJobStatus(client0, jobId), completed(jobId));
    }

    @Test
    void shouldReturnStatusOfRemoteJob() {
        IgniteImpl entryNode = CLUSTER.node(0);

        JobExecution<String> execution = runBlockingJob(entryNode, Set.of(CLUSTER.node(1).node()));

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        unblockJob();

        await().until(() -> getJobStatus(client0, jobId), completed(jobId));
    }

    @Test
    void shouldReturnProblemIfStatusOfNonExistingJob() {
        UUID jobId = UUID.randomUUID();

        HttpClientResponseException httpClientResponseException = assertThrows(
                HttpClientResponseException.class,
                () -> getJobStatus(client0, jobId)
        );

        assertThat(
                httpClientResponseException.getResponse(),
                MicronautHttpResponseMatcher.<Problem>hasStatusCode(404)
                        .withBody(isProblem().withStatus(404).withDetail("Compute job not found [jobId=" + jobId + "]"), Problem.class)
        );
    }

    @Test
    void shouldCancelJobLocally() {
        IgniteImpl entryNode = CLUSTER.node(0);

        JobExecution<String> execution = runBlockingJob(entryNode, Set.of(entryNode.node()));

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        cancelJob(client0, jobId);

        await().until(() -> getJobStatus(client0, jobId), canceled(jobId, true));
    }

    @Test
    void shouldCancelJobRemotely() {
        IgniteImpl entryNode = CLUSTER.node(0);

        JobExecution<String> execution = runBlockingJob(entryNode, Set.of(CLUSTER.node(1).node()));

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        cancelJob(client0, jobId);

        await().until(() -> getJobStatus(client0, jobId), canceled(jobId, true));
    }

    @Test
    void shouldReturnProblemIfCancelNonExistingJob() {
        UUID jobId = UUID.randomUUID();

        HttpClientResponseException httpClientResponseException = assertThrows(
                HttpClientResponseException.class,
                () -> cancelJob(client0, jobId)
        );

        assertThat(
                httpClientResponseException.getResponse(),
                MicronautHttpResponseMatcher.<Problem>hasStatusCode(404)
                        .withBody(isProblem().withStatus(404).withDetail("Compute job not found [jobId=" + jobId + "]"), Problem.class)
        );
    }

    @Test
    void shouldReturnFalseIfCancelCompletedJob() {
        IgniteImpl entryNode = CLUSTER.node(0);

        JobExecution<String> execution = runBlockingJob(entryNode, Set.of(entryNode.node()));

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        unblockJob();

        await().until(() -> getJobStatus(client0, jobId), completed(jobId));

        HttpClientResponseException httpClientResponseException = assertThrows(
                HttpClientResponseException.class,
                () -> cancelJob(client0, jobId)
        );

        assertThat(
                httpClientResponseException.getResponse(),
                MicronautHttpResponseMatcher.<Problem>hasStatusCode(409)
                        .withBody(isProblem().withStatus(409)
                                .withDetail("Compute job is in illegal state [jobId=" + jobId + ", state=COMPLETED]"), Problem.class)
        );
    }

    @Test
    void shouldUpdatePriorityLocally() {
        IgniteImpl entryNode = CLUSTER.node(0);

        Set<ClusterNode> nodes = Set.of(entryNode.node());

        JobExecution<String> execution = runBlockingJob(entryNode, nodes);

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        JobExecution<String> execution2 = runBlockingJob(entryNode, nodes);

        UUID jobId2 = execution2.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId2), queued(jobId2));

        updatePriority(client0, jobId2, 1);
    }

    @Test
    void shouldUpdatePriorityRemotely() {
        IgniteImpl entryNode = CLUSTER.node(0);

        Set<ClusterNode> nodes = Set.of(CLUSTER.node(1).node());

        JobExecution<String> execution = runBlockingJob(entryNode, nodes);

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        JobExecution<String> execution2 = runBlockingJob(entryNode, nodes);

        UUID jobId2 = execution2.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId2), queued(jobId2));

        updatePriority(client0, jobId2, 1);
    }

    @Test
    void shouldReturnProblemIfUpdatePriorityOfNonExistingJob() {
        UUID jobId = UUID.randomUUID();

        HttpClientResponseException httpClientResponseException = assertThrows(
                HttpClientResponseException.class,
                () -> updatePriority(client0, jobId, 1)
        );

        assertThat(
                httpClientResponseException.getResponse(),
                MicronautHttpResponseMatcher.<Problem>hasStatusCode(404)
                        .withBody(isProblem().withStatus(404).withDetail("Compute job not found [jobId=" + jobId + "]"), Problem.class)
        );
    }

    @Test
    void shouldReturnFalseIfUpdatePriorityOfRunningJob() {
        IgniteImpl entryNode = CLUSTER.node(0);

        Set<ClusterNode> nodes = Set.of(entryNode.node());

        JobExecution<String> execution = runBlockingJob(entryNode, nodes);

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        HttpClientResponseException httpClientResponseException = assertThrows(
                HttpClientResponseException.class,
                () -> updatePriority(client0, jobId, 1)
        );

        assertThat(
                httpClientResponseException.getResponse(),
                MicronautHttpResponseMatcher.<Problem>hasStatusCode(409)
                        .withBody(isProblem().withStatus(409)
                                .withDetail("Compute job is in illegal state [jobId=" + jobId + ", state=EXECUTING]"), Problem.class)
        );
    }

    @Test
    void shouldReturnFalseIfUpdatePriorityOfCompletedJob() {
        IgniteImpl entryNode = CLUSTER.node(0);

        Set<ClusterNode> nodes = Set.of(entryNode.node());

        JobExecution<String> execution = runBlockingJob(entryNode, nodes);

        UUID jobId = execution.idAsync().join();

        await().until(() -> getJobStatus(client0, jobId), executing(jobId));

        unblockJob();

        await().until(() -> getJobStatus(client0, jobId), completed(jobId));

        HttpClientResponseException httpClientResponseException = assertThrows(
                HttpClientResponseException.class,
                () -> updatePriority(client0, jobId, 1)
        );

        assertThat(
                httpClientResponseException.getResponse(),
                MicronautHttpResponseMatcher.<Problem>hasStatusCode(409)
                        .withBody(isProblem().withStatus(409)
                                .withDetail("Compute job is in illegal state [jobId=" + jobId + ", state=COMPLETED]"), Problem.class)
        );
    }

    private static JobExecution<String> runBlockingJob(IgniteImpl entryNode, Set<ClusterNode> nodes) {
        return entryNode.compute().submit(nodes, List.of(), BlockingJob.class.getName());
    }

    private static void unblockJob() {
        synchronized (LOCK) {
            LOCK.notifyAll();
        }
    }

    private static Map<UUID, JobStatus> getJobStatuses(HttpClient client) {
        List<JobStatus> statuses = client.toBlocking()
                .retrieve(HttpRequest.GET("/jobs"), Argument.listOf(JobStatus.class));

        return statuses.stream().collect(Collectors.toMap(JobStatus::id, s -> s));
    }

    private static JobStatus getJobStatus(HttpClient client, UUID jobId) {
        return client.toBlocking().retrieve("/jobs/" + jobId, JobStatus.class);
    }

    private static void updatePriority(HttpClient client, UUID jobId, int priority) {
        client.toBlocking()
                .exchange(PUT("/jobs/" + jobId + "/priority", new UpdateJobPriorityBody(priority)));
    }

    private static void cancelJob(HttpClient client, UUID jobId) {
        client.toBlocking().exchange(DELETE("/jobs/" + jobId));
    }

    private static class BlockingJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Object... args) {
            synchronized (LOCK) {
                try {
                    LOCK.wait();
                } catch (InterruptedException e) {
                    // No-op.
                }
            }

            return null;
        }
    }
}
