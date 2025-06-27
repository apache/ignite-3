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

package org.apache.ignite.deployment;

import static org.apache.ignite.deployment.version.Version.parseVersion;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.FileDeployerService;
import org.apache.ignite.internal.deployunit.UnitContent;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link FileDeployerService}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FileDeployerServiceTest {
    private final FileDeployerService service = new FileDeployerService("test");

    @WorkDirectory
    private Path workDir;

    private Path file1;
    private Path file2;
    private Path file3;

    @BeforeEach
    public void setup() throws IOException {
        service.initUnitsFolder(workDir);

        file1 = workDir.resolve("file1");
        file2 = workDir.resolve("file2");
        file3 = workDir.resolve("file3");
        IgniteTestUtils.fillDummyFile(file1, 1024);
        IgniteTestUtils.fillDummyFile(file2, 1024);
        IgniteTestUtils.fillDummyFile(file3, 1024);
    }

    @Test
    public void test() throws Exception {
        try (DeploymentUnit unit = content()) {
            CompletableFuture<Boolean> deployed = service.deploy("id", parseVersion("1.0.0"), unit);
            assertThat(deployed, willBe(true));
        }

        try (DeploymentUnit unit = content()) {
            CompletableFuture<UnitContent> unitContent = service.getUnitContent("id", parseVersion("1.0.0"));
            assertThat(unitContent, willBe(equalTo(UnitContent.readContent(unit))));
        }
    }

    private DeploymentUnit content() {
        Map<String, InputStream> map = Stream.of(file1, file2, file3)
                .collect(Collectors.toMap(it -> it.getFileName().toString(), it -> {
                    try {
                        byte[] buf = Files.readAllBytes(it);
                        if (buf.length == 0) {
                            throw new RuntimeException(new FileNotFoundException(it.toString()));
                        }
                        return new ByteArrayInputStream(buf);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));

        return new DeploymentUnit(map);
    }
}
