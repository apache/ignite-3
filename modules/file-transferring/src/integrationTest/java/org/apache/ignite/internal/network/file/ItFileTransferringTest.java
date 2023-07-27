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

package org.apache.ignite.internal.network.file;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.network.file.FileAssertions.assertContentEquals;
import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.file.TestCluster.Node;
import org.apache.ignite.internal.network.file.messages.TransferMetadata;
import org.apache.ignite.internal.network.file.messages.TransferMetadataImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for file transferring.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItFileTransferringTest {

    @WorkDirectory
    private Path workDir;

    private TestCluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) throws InterruptedException {
        cluster = new TestCluster(2, workDir, testInfo);
        cluster.startAwait();
    }

    @AfterEach
    void tearDown() throws Exception {
        cluster.shutdown();
    }

    @Test
    void download() throws IOException {
        Node node0 = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = node0.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        File file1 = randomFile(unitPath, 1024 * 1024 * 10).toFile();
        File file2 = randomFile(unitPath, 1024 * 1024 * 10 + 1).toFile();
        File file3 = randomFile(unitPath, 1024 * 1024 * 10 - 1).toFile();

        node0.fileTransferringService().addFileProvider(
                TransferMetadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node node1 = cluster.members.get(1);
        CompletableFuture<Path> download = node1.fileTransferringService().download(
                node0.nodeName(),
                TransferMetadataImpl.builder().build()
        );
        assertThat(download.thenAccept(path -> assertContentEquals(unitPath, path)), CompletableFutureMatcher.willCompleteSuccessfully());
    }

    @Test
    void upload() throws IOException {
        Node node0 = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = node0.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        File file1 = randomFile(unitPath, 1024 * 1024 * 10).toFile();
        File file2 = randomFile(unitPath, 1024 * 1024 * 10 + 1).toFile();
        File file3 = randomFile(unitPath, 1024 * 1024 * 10 - 1).toFile();

        node0.fileTransferringService().addFileProvider(
                TransferMetadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node node1 = cluster.members.get(1);

        CompletableFuture<Path> future = new CompletableFuture<>();
        node1.fileTransferringService().addFileHandler(TransferMetadata.class, ((metadata, uploadedFile) -> {
            future.complete(uploadedFile);
            return completedFuture(null);
        }));

        node0.fileTransferringService().upload(node1.nodeName(), TransferMetadataImpl.builder().build());

        assertThat(future.thenAccept(path -> assertContentEquals(unitPath, path)), CompletableFutureMatcher.willCompleteSuccessfully());
        await().until(() -> !Files.exists(future.get()));
    }
}
