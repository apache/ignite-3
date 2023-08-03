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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.network.file.FileAssertions.assertContentEquals;
import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.FileTransferringConfiguration;
import org.apache.ignite.internal.network.file.TestCluster.Node;
import org.apache.ignite.internal.network.file.messages.Metadata;
import org.apache.ignite.internal.network.file.messages.MetadataImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for file transferring.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class ItFileTransferTest {

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private FileTransferringConfiguration configuration;

    private TestCluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) throws InterruptedException {
        cluster = new TestCluster(2, configuration, workDir, testInfo);
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

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, chunkSize - 1).toFile();
        File file2 = randomFile(unitPath, chunkSize + 1).toFile();
        File file3 = randomFile(unitPath, chunkSize * 2).toFile();

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node node1 = cluster.members.get(1);
        CompletableFuture<List<File>> downloadedFilesFuture = node1.fileTransferringService().download(
                node0.nodeName(),
                MetadataImpl.builder().build()
        );

        assertThat(
                downloadedFilesFuture.thenAccept(files -> {
                    assertContentEquals(Set.of(file1, file2, file3), new HashSet<>(files));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void downloadNonReadableFiles() throws IOException {
        Node node0 = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = node0.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, chunkSize - 1).toFile();
        File file2 = randomFile(unitPath, chunkSize + 1).toFile();
        File file3 = randomFile(unitPath, chunkSize * 2).toFile();

        assertTrue(file2.setReadable(false));

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node node1 = cluster.members.get(1);
        CompletableFuture<List<File>> download = node1.fileTransferringService().download(
                node0.nodeName(),
                MetadataImpl.builder().build()
        );
        assertThat(download, willThrow(FileTransferException.class, "Permission denied"));
    }

    @Test
    void downloadFilesWhenProviderThrowsException() {
        Node node0 = cluster.members.get(0);

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        Node node1 = cluster.members.get(1);
        CompletableFuture<List<File>> download = node1.fileTransferringService().download(
                node0.nodeName(),
                MetadataImpl.builder().build()
        );
        assertThat(download, willThrowWithCauseOrSuppressed(FileTransferException.class));
    }

    @Test
    void downloadEmptyFileList() {
        Node node0 = cluster.members.get(0);

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of())
        );

        Node node1 = cluster.members.get(1);
        CompletableFuture<List<File>> downloadedFiles = node1.fileTransferringService().download(
                node0.nodeName(),
                MetadataImpl.builder().build()
        );
        assertThat(downloadedFiles.thenAccept(files -> assertThat(files, hasSize(0))), willCompleteSuccessfully());
    }

    @Test
    void upload() throws IOException {
        Node node0 = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = node0.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, chunkSize - 1).toFile();
        File file2 = randomFile(unitPath, chunkSize + 1).toFile();
        File file3 = randomFile(unitPath, chunkSize * 2).toFile();

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node node1 = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        node1.fileTransferringService().addFileHandler(Metadata.class, ((metadata, uploadedFilesDir) -> {
            uploadedFilesFuture.complete(uploadedFilesDir);
            return completedFuture(null);
        }));

        node0.fileTransferringService().upload(node1.nodeName(), MetadataImpl.builder().build());

        assertThat(
                uploadedFilesFuture.thenAccept(files -> {
                    assertContentEquals(Set.of(file1, file2, file3), new HashSet<>(files));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void uploadEmptyFileList() {
        Node node0 = cluster.members.get(0);

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of())
        );

        Node node1 = cluster.members.get(1);

        assertThat(
                node0.fileTransferringService().upload(node1.nodeName(), MetadataImpl.builder().build()),
                willThrow(FileTransferException.class, "No files to upload")
        );
    }

    @Test
    void uploadNonReadableFiles() throws IOException {
        Node node0 = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = node0.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, chunkSize - 1).toFile();
        File file2 = randomFile(unitPath, chunkSize + 1).toFile();
        File file3 = randomFile(unitPath, chunkSize * 2).toFile();

        assertTrue(file2.setReadable(false));

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node node1 = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        node1.fileTransferringService().addFileHandler(Metadata.class, ((metadata, uploadedFilesDir) -> {
            uploadedFilesFuture.complete(uploadedFilesDir);
            return completedFuture(null);
        }));

        assertThat(
                node0.fileTransferringService().upload(node1.nodeName(), MetadataImpl.builder().build()),
                willThrow(FileTransferException.class, "Failed to send files to node: ")
        );

        assertThat(
                uploadedFilesFuture,
                willTimeoutIn(1, TimeUnit.SECONDS)
        );
    }

    @Test
    void uploadFilesWhenProviderThrowsException() {
        Node node0 = cluster.members.get(0);

        node0.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        Node node1 = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        node1.fileTransferringService().addFileHandler(Metadata.class, ((metadata, uploadedFiles) -> {
            uploadedFilesFuture.complete(uploadedFiles);
            return completedFuture(null);
        }));

        assertThat(
                node0.fileTransferringService().upload(node1.nodeName(), MetadataImpl.builder().build()),
                willThrow(RuntimeException.class, "Test exception")
        );

        assertThat(
                uploadedFilesFuture,
                willTimeoutIn(1, TimeUnit.SECONDS)
        );
    }
}
