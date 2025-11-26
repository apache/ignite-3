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

package org.apache.ignite.migrationtools.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.apache.ignite.migrationtools.tests.containers.Ignite3ClusterContainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/** Tests loading the converted configurations in a cluster. */
public class ClusterLoadsConfigTest {
    private static Stream<String> allCacheExamples() {
        return ConfigExamples.configPaths();
    }

    @ParameterizedTest
    @MethodSource("allCacheExamples")
    void dockerRunsSuccessfully(String inputPath) throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile(inputPath);
        var nodeCfgPath = testDescr.getNodeCfgPath();

        String imageName = Ignite3ClusterContainer.dockerImageName();
        try (var container = new GenericContainer(imageName)
                .withCommand("--config-path=/node.cfg")
                .withCopyToContainer(MountableFile.forHostPath(nodeCfgPath), "/node.cfg")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forLogMessage(".*Components started.*", 1)
                        .withStartupTimeout(Duration.of(30, ChronoUnit.SECONDS)))) {

            container.start();
            assertThat(container.isRunning()).isTrue();
        }
    }
}
