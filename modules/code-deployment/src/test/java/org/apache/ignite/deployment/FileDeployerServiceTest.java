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

import static org.apache.ignite.compute.version.Version.parseVersion;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.deployunit.FileDeployerService;
import org.apache.ignite.internal.deployunit.UnitContent;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link FileDeployerService}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FileDeployerServiceTest {
    private final FileDeployerService service = new FileDeployerService();

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
        IgniteUtils.fillDummyFile(file1, 1024);
        IgniteUtils.fillDummyFile(file2, 1024);
        IgniteUtils.fillDummyFile(file3, 1024);
    }

    @Test
    public void test() throws IOException {
        CompletableFuture<Boolean> deployed = service.deploy("id", parseVersion("1.0.0"), content());
        assertThat(deployed, willBe(true));

        CompletableFuture<UnitContent> unitContent = service.getUnitContent("id", parseVersion("1.0.0"));
        assertThat(unitContent, willBe(equalTo(content())));
    }

    private UnitContent content() throws IOException {
        byte[] content1 = Files.readAllBytes(file1);
        byte[] content2 = Files.readAllBytes(file2);
        byte[] content3 = Files.readAllBytes(file3);

        return new UnitContent(Map.of(file1.getFileName().toString(), content1,
                file2.getFileName().toString(), content2,
                file3.getFileName().toString(), content3));
    }
}
