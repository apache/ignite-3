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

package org.apache.ignite.internal.cli.call.unit;

import static org.apache.ignite.internal.cli.call.unit.DeployUndeployTestSupport.createEmptyFileIn;
import static org.apache.ignite.internal.cli.call.unit.DeployUndeployTestSupport.get;
import static org.apache.ignite.internal.cli.call.unit.DeployUndeployTestSupport.tracker;
import static org.apache.ignite.rest.client.model.DeploymentStatus.DEPLOYED;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import jakarta.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.call.cluster.unit.ClusterListUnitCall;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCallFactory;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCallInput;
import org.apache.ignite.internal.cli.call.cluster.unit.UndeployUnitCall;
import org.apache.ignite.internal.cli.call.cluster.unit.UndeployUnitCallInput;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.UnitAlreadyExistsException;
import org.apache.ignite.internal.cli.core.exception.UnitNotFoundException;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.model.UnitStatus;
import org.apache.ignite.rest.client.model.UnitVersionStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Integration test for the deployment lifecycle: deploy, list, check status, undeploy. */
public class ItDeployUndeployCallsTest extends CliIntegrationTest {

    @Inject
    DeployUnitCallFactory deployUnitCallFactory;

    @Inject
    ClusterListUnitCall listUnitCall;

    @Inject
    UndeployUnitCall undeployUnitCall;

    private static ListUnitCallInput listAllInput() {
        return ListUnitCallInput.builder()
                .url(NODE_URL)
                .build();
    }

    private static ListUnitCallInput listIdInput(String id) {
        return ListUnitCallInput.builder()
                .unitId(id)
                .url(NODE_URL)
                .build();
    }

    private static DeployUnitCallInput deployInput(String id, String version) {
        return DeployUnitCallInput.builder()
                .id(id)
                .version(version)
                .path(createEmptyFileIn(WORK_DIR))
                .clusterUrl(NODE_URL)
                .build();
    }

    private static UndeployUnitCallInput undeployInput(String id, String version) {
        return UndeployUnitCallInput.builder()
                .id(id)
                .version(version)
                .clusterUrl(NODE_URL)
                .build();
    }

    @Test
    @DisplayName("Base test for the deployment lifecycle: deploy, list, check status, undeploy")
    void deployListStatusUndeploy() {
        // Given no units deployed
        assertThat(listUnitCall.execute(listAllInput()).isEmpty()).isTrue();

        // When deploy unit
        CallOutput<String> deployOutput = get(
                deployUnitCallFactory.create(tracker()).execute(deployInput("test.id", "1.0.0"))
        );

        // Then
        assertThat(deployOutput.hasError()).isFalse();
        assertThat(deployOutput.body()).isEqualTo(MessageUiComponent.from(UiElements.done()).render());

        await().untilAsserted(() -> {
            // And list is not empty
            List<UnitStatus> unitsStatuses = listUnitCall.execute(listAllInput()).body();
            assertThat(unitsStatuses.size()).isEqualTo(1);
            Assertions.assertThat(unitsStatuses.get(0).getVersionToStatus())
                    .containsExactly((new UnitVersionStatus()).version("1.0.0").status(DEPLOYED));
        });

        // When undeploy unit
        CallOutput<String> undeployOutput = undeployUnitCall.execute(undeployInput("test.id", "1.0.0"));
        assertThat(undeployOutput.hasError()).isFalse();
        assertThat(undeployOutput.body()).isEqualTo(MessageUiComponent.from(UiElements.done()).render());

        // Then list is empty
        await().untilAsserted(() -> assertThat(listUnitCall.execute(listAllInput()).isEmpty()).isTrue());
    }

    @Test
    @DisplayName("Should return error when wrong unit path is provided")
    void wrongUnitPathWhenDeploy() {
        // Given input with wrong path
        DeployUnitCallInput input = DeployUnitCallInput.builder()
                .path(Path.of("wrong/path"))
                .id("test.id").version("1.0.0").clusterUrl(NODE_URL).build();

        // When
        CallOutput<String> output = get(deployUnitCallFactory.create(tracker()).execute(input));

        // Then
        assertThat(output.hasError()).isTrue();
        assertThat(output.errorCause()).isInstanceOf(FileNotFoundException.class);
    }

    @Test
    @DisplayName("Should return error when there is no unit with such id (undeploy)")
    void noUnitWithSuchIdWhenUndeploy() {
        // Given input with wrong unit id
        UndeployUnitCallInput input = undeployInput("no.such.unit", "1.0.0");

        // When
        CallOutput<String> output = undeployUnitCall.execute(input);

        // Then
        assertThat(output.hasError()).isTrue();
        assertThat(output.errorCause()).isInstanceOf(UnitNotFoundException.class);
        var err = (UnitNotFoundException) output.errorCause();
        assertThat(err.unitId()).isEqualTo("no.such.unit");
        assertThat(err.version()).isEqualTo("1.0.0");
    }

    @Test
    @DisplayName("Should return empty list when there is no unit with such id (list)")
    void noUnitWithSuchIdWhenList() {
        // Given input with wrong unit id
        ListUnitCallInput input = listIdInput("no.such.unit");

        // Then
        await().untilAsserted(() -> assertThat(listUnitCall.execute(input).isEmpty()).isTrue());
    }

    @Test
    @DisplayName("Should return error when there is such unit already deployed")
    void unitAlreadyDeployed() {
        // Given unit deployed
        get(deployUnitCallFactory.create(tracker()).execute(deployInput("test.id", "1.0.0")));

        // When try to deploy it again
        CallOutput<String> output = get(deployUnitCallFactory.create(tracker()).execute(deployInput("test.id", "1.0.0")));

        // Then
        assertThat(output.hasError()).isTrue();
        assertThat(output.errorCause()).isInstanceOf(UnitAlreadyExistsException.class);
        var err = (UnitAlreadyExistsException) output.errorCause();
        assertThat(err.unitId()).isEqualTo("test.id");
        assertThat(err.version()).isEqualTo("1.0.0");

        // Cleanup
        CallOutput<String> undeployOutput = undeployUnitCall.execute(undeployInput("test.id", "1.0.0"));
        assertThat(undeployOutput.hasError()).isFalse();
        assertThat(undeployOutput.body()).isEqualTo(MessageUiComponent.from(UiElements.done()).render());

        // Wait for cleanup
        await().untilAsserted(() -> assertThat(listUnitCall.execute(listAllInput()).isEmpty()).isTrue());
    }

    @Test
    @DisplayName("Should return error when wrong version format provided")
    void wrongVersionFormat() {
        // Given input with wrong version format
        DeployUnitCallInput input = DeployUnitCallInput.builder()
                .path(createEmptyFileIn(WORK_DIR))
                .id("test.id").version("1.myversion").clusterUrl(NODE_URL).build();

        // When
        CallOutput<String> output = get(deployUnitCallFactory.create(tracker()).execute(input));

        // Then
        assertThat(output.hasError()).isTrue();
        assertThat(output.errorCause()).isInstanceOf(IgniteCliApiException.class);
    }

    @Test
    @DisplayName("Should return unit status after unit deploy")
    void deployStatusCheck() {
        // When deploy unit
        CallOutput<String> deployOutput = get(
                deployUnitCallFactory.create(tracker()).execute(deployInput("test.id", "1.1.0"))
        );

        // Then
        assertThat(deployOutput.hasError()).isFalse();
        assertThat(deployOutput.body()).isEqualTo(MessageUiComponent.from(UiElements.done()).render());

        await().untilAsserted(() -> {
            // And list is not empty
            List<UnitStatus> unisStatuses = listUnitCall.execute(listAllInput()).body();
            assertThat(unisStatuses.size()).isEqualTo(1);
            Assertions.assertThat(unisStatuses.get(0).getVersionToStatus())
                    .containsExactly((new UnitVersionStatus()).version("1.1.0").status(DEPLOYED));
        });
    }

    @Test
    @DisplayName("Should deploy unit recursively from directory with subdirectories")
    void deployRecursive() throws IOException {
        // Given a directory with subdirectories
        Path recursiveDir = Files.createTempDirectory(WORK_DIR, "recursive");
        Path subDir = Files.createDirectory(recursiveDir.resolve("subdir"));
        Files.createFile(recursiveDir.resolve("root.txt"));
        Files.createFile(subDir.resolve("nested.txt"));

        DeployUnitCallInput input = DeployUnitCallInput.builder()
                .id("recursive.test.id")
                .version("1.0.0")
                .path(recursiveDir)
                .recursive(true)
                .clusterUrl(NODE_URL)
                .build();

        // When deploy unit recursively
        CallOutput<String> deployOutput = get(
                deployUnitCallFactory.create(tracker()).execute(input)
        );

        // Then
        assertThat(deployOutput.hasError()).isFalse();
        assertThat(deployOutput.body()).isEqualTo(MessageUiComponent.from(UiElements.done()).render());

        await().untilAsserted(() -> {
            // And list contains the deployed unit
            List<UnitStatus> unitStatuses = listUnitCall.execute(listIdInput("recursive.test.id")).body();
            assertThat(unitStatuses.size()).isEqualTo(1);
            Assertions.assertThat(unitStatuses.get(0).getVersionToStatus())
                    .containsExactly((new UnitVersionStatus()).version("1.0.0").status(DEPLOYED));
        });
    }

    @Test
    @DisplayName("Should deploy unit recursively from deeply nested directory")
    void deployRecursiveDeepNesting() throws IOException {
        // Given a directory with deeply nested subdirectories
        Path deepDir = Files.createTempDirectory(WORK_DIR, "deep");
        Path level1 = Files.createDirectory(deepDir.resolve("level1"));
        Path level2 = Files.createDirectory(level1.resolve("level2"));
        Files.createFile(deepDir.resolve("root.txt"));
        Files.createFile(level1.resolve("file1.txt"));
        Files.createFile(level2.resolve("file2.txt"));

        DeployUnitCallInput input = DeployUnitCallInput.builder()
                .id("deep.recursive.test.id")
                .version("1.0.0")
                .path(deepDir)
                .recursive(true)
                .clusterUrl(NODE_URL)
                .build();

        // When deploy unit recursively
        CallOutput<String> deployOutput = get(
                deployUnitCallFactory.create(tracker()).execute(input)
        );

        // Then
        assertThat(deployOutput.hasError()).isFalse();
        assertThat(deployOutput.body()).isEqualTo(MessageUiComponent.from(UiElements.done()).render());

        await().untilAsserted(() -> {
            // And list contains the deployed unit
            List<UnitStatus> unitStatuses = listUnitCall.execute(listIdInput("deep.recursive.test.id")).body();
            assertThat(unitStatuses.size()).isEqualTo(1);
            Assertions.assertThat(unitStatuses.get(0).getVersionToStatus())
                    .containsExactly((new UnitVersionStatus()).version("1.0.0").status(DEPLOYED));
        });
    }
}
