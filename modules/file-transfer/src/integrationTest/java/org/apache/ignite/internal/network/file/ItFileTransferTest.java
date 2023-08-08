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
import static org.apache.ignite.internal.network.file.FileUtils.sortByNames;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.FileTransferConfiguration;
import org.apache.ignite.internal.network.file.TestCluster.Node;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
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

    @InjectConfiguration("mock.chunkSize=1024")
    private FileTransferConfiguration configuration;

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
        Node targetNode = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = targetNode.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, 0);
        File file2 = randomFile(unitPath, chunkSize + 1);
        File file3 = randomFile(unitPath, chunkSize * 2);
        File file4 = randomFile(unitPath, chunkSize * 2);

        targetNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3, file4))
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> downloadedFilesFuture = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                MetadataImpl.builder().build()
        );

        assertThat(
                downloadedFilesFuture.thenAccept(files -> {
                    assertContentEquals(sortByNames(file1, file2, file3, file4), sortByNames(files));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void downloadNonReadableFiles() throws IOException {
        Node targetNode = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = targetNode.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, chunkSize - 1);
        File file2 = randomFile(unitPath, chunkSize + 1);
        File file3 = randomFile(unitPath, chunkSize * 2);

        assertTrue(file2.setReadable(false));

        targetNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> download = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                MetadataImpl.builder().build()
        );
        assertThat(download, willThrow(FileTransferException.class, "Permission denied"));
    }

    @Test
    void downloadFilesWhenProviderThrowsException() {
        Node targetNode = cluster.members.get(0);

        targetNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> download = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                MetadataImpl.builder().build()
        );
        assertThat(download, willThrowWithCauseOrSuppressed(FileTransferException.class));
    }

    @Test
    void downloadFilesWhenDoNotHaveAccessToWrite() throws IOException {
        Node targetNode = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = targetNode.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, 0);
        File file2 = randomFile(unitPath, chunkSize + 1);
        File file3 = randomFile(unitPath, chunkSize * 2);
        File file4 = randomFile(unitPath, chunkSize * 2);

        targetNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3, file4))
        );

        Node sourceNode = cluster.members.get(1);
        assertTrue(sourceNode.workDir().toFile().setWritable(false));

        CompletableFuture<List<File>> downloadedFilesFuture = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                MetadataImpl.builder().build()
        );

        assertThat(
                downloadedFilesFuture.thenAccept(files -> {
                    assertContentEquals(sortByNames(file1, file2, file3, file4), sortByNames(files));
                }),
                willThrowWithCauseOrSuppressed(AccessDeniedException.class)
        );
    }

    @Test
    void downloadEmptyFileList() {
        Node targetNode = cluster.members.get(0);

        targetNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of())
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> downloadedFiles = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                MetadataImpl.builder().build()
        );
        assertThat(downloadedFiles, willThrow(FileTransferException.class, "No files to download"));
    }

    @Test
    void upload() throws IOException {
        Node sourceNode = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = sourceNode.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, 0);
        File file2 = randomFile(unitPath, chunkSize - 1);
        File file3 = randomFile(unitPath, chunkSize + 1);
        File file4 = randomFile(unitPath, chunkSize * 2);

        sourceNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3, file4))
        );

        Node targetNode = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferringService().addFileHandler(Metadata.class, ((metadata, uploadedFilesDir) -> {
            uploadedFilesFuture.complete(uploadedFilesDir);
            return completedFuture(null);
        }));

        sourceNode.fileTransferringService().upload(targetNode.nodeName(), MetadataImpl.builder().build());

        assertThat(
                uploadedFilesFuture.thenAccept(files -> {
                    assertContentEquals(sortByNames(file1, file2, file3, file4), sortByNames(files));
                }),
                willCompleteSuccessfully()
        );
    }

    @Test
    void uploadEmptyFileList() {
        Node sourceNode = cluster.members.get(0);

        sourceNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of())
        );

        Node targetNode = cluster.members.get(1);

        assertThat(
                sourceNode.fileTransferringService().upload(targetNode.nodeName(), MetadataImpl.builder().build()),
                willThrow(FileTransferException.class, "No files to upload")
        );
    }

    @Test
    void uploadNonReadableFiles() throws IOException {
        Node sourceNode = cluster.members.get(0);

        String unit = "unit";
        String version = "1.0.0";
        Path unitPath = sourceNode.workDir().resolve(unit).resolve(version);
        Files.createDirectories(unitPath);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(unitPath, chunkSize - 1);
        File file2 = randomFile(unitPath, chunkSize + 1);
        File file3 = randomFile(unitPath, chunkSize * 2);

        assertTrue(file2.setReadable(false));

        sourceNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node targetNode = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferringService().addFileHandler(Metadata.class, ((metadata, uploadedFilesDir) -> {
            uploadedFilesFuture.complete(uploadedFilesDir);
            return completedFuture(null);
        }));

        assertThat(
                sourceNode.fileTransferringService().upload(targetNode.nodeName(), MetadataImpl.builder().build()),
                willThrow(FileTransferException.class, "Failed to send files to node: ")
        );

        assertThat(
                uploadedFilesFuture,
                willTimeoutIn(1, TimeUnit.SECONDS)
        );
    }

    @Test
    void uploadFilesWhenProviderThrowsException() {
        Node sourceNode = cluster.members.get(0);

        sourceNode.fileTransferringService().addFileProvider(
                Metadata.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        Node targetNode = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferringService().addFileHandler(Metadata.class, ((metadata, uploadedFiles) -> {
            uploadedFilesFuture.complete(uploadedFiles);
            return completedFuture(null);
        }));

        assertThat(
                sourceNode.fileTransferringService().upload(targetNode.nodeName(), MetadataImpl.builder().build()),
                willThrow(RuntimeException.class, "Test exception")
        );

        assertThat(
                uploadedFilesFuture,
                willTimeoutIn(1, TimeUnit.SECONDS)
        );
    }
}
