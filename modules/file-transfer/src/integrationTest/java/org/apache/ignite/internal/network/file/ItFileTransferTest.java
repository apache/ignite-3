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
import static org.apache.ignite.internal.network.file.FileAssertions.assertNamesAndContentEquals;
import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.Identifier;
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
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class ItFileTransferTest {
    @WorkDirectory
    private Path workDir;

    @InjectConfiguration("mock.chunkSize=1024")
    private FileTransferConfiguration configuration;

    private TestCluster cluster;

    private Path sourceDir;

    private final FileTransferFactory factory = new FileTransferFactory();

    @BeforeEach
    void setUp(TestInfo testInfo) throws InterruptedException, IOException {
        sourceDir = Files.createDirectories(workDir.resolve("source"));

        cluster = new TestCluster(2, configuration, workDir, testInfo);
        cluster.startAwait();
    }

    @AfterEach
    void tearDown() throws Exception {
        cluster.shutdown();
    }

    @Test
    void download() {
        Node targetNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(sourceDir, chunkSize);
        File file2 = randomFile(sourceDir, chunkSize + 1);
        File file3 = randomFile(sourceDir, chunkSize * 2);
        File file4 = randomFile(sourceDir, chunkSize * 2);

        List<File> files = List.of(file1, file2, file3, file4);
        targetNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(files)
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> downloadedFilesFuture = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                factory.identifier().build()
        );

        assertThat(
                downloadedFilesFuture,
                willBe(assertNamesAndContentEquals(files))
        );
    }

    @Test
    void downloadNonReadableFiles() {
        Node targetNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(sourceDir, chunkSize - 1);
        File file2 = randomFile(sourceDir, chunkSize + 1);
        File file3 = randomFile(sourceDir, chunkSize * 2);

        assertTrue(file2.setReadable(false));

        targetNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> download = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                factory.identifier().build()
        );
        assertThat(download, willThrow(FileTransferException.class, "Permission denied"));
    }

    @Test
    void downloadFilesWhenProviderThrowsException() {
        Node targetNode = cluster.members.get(0);

        targetNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> download = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                factory.identifier().build()
        );
        assertThat(download, willThrowWithCauseOrSuppressed(FileTransferException.class));
    }

    @Test
    void downloadFilesWhenDoNotHaveAccessToWrite() {
        Node targetNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(sourceDir, 0);
        File file2 = randomFile(sourceDir, chunkSize + 1);
        File file3 = randomFile(sourceDir, chunkSize * 2);
        File file4 = randomFile(sourceDir, chunkSize * 2);

        targetNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3, file4))
        );

        Node sourceNode = cluster.members.get(1);
        assertTrue(sourceNode.workDir().toFile().setWritable(false));

        CompletableFuture<List<File>> downloadedFilesFuture = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                factory.identifier().build()
        );

        assertThat(
                downloadedFilesFuture,
                willThrowWithCauseOrSuppressed(AccessDeniedException.class)
        );
    }

    @Test
    void downloadEmptyFileList() {
        Node targetNode = cluster.members.get(0);

        targetNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of())
        );

        Node sourceNode = cluster.members.get(1);
        CompletableFuture<List<File>> downloadedFiles = sourceNode.fileTransferringService().download(
                targetNode.nodeName(),
                factory.identifier().build()
        );
        assertThat(downloadedFiles, willThrow(FileTransferException.class, "No files to download"));
    }

    @Test
    void upload() {
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(sourceDir, chunkSize);
        File file2 = randomFile(sourceDir, chunkSize - 1);
        File file3 = randomFile(sourceDir, chunkSize + 1);
        File file4 = randomFile(sourceDir, chunkSize * 2);

        List<File> files = List.of(file1, file2, file3, file4);
        sourceNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(files)
        );

        Node targetNode = cluster.members.get(1);

        CompletableFuture<Void> uploadedFilesAssertion = new CompletableFuture<>();
        targetNode.fileTransferringService().addFileConsumer(Identifier.class, ((metadata, uploadedFiles) -> {
            return uploadedFilesAssertion.completeAsync(() -> {
                assertThat(uploadedFiles, assertNamesAndContentEquals(files));
                return null;
            });
        }));

        sourceNode.fileTransferringService().upload(targetNode.nodeName(), factory.identifier().build());

        assertThat(
                uploadedFilesAssertion,
                CompletableFutureMatcher.willCompleteSuccessfully()
        );
    }

    @Test
    void uploadEmptyFileList() {
        Node sourceNode = cluster.members.get(0);

        sourceNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of())
        );

        Node targetNode = cluster.members.get(1);

        assertThat(
                sourceNode.fileTransferringService().upload(targetNode.nodeName(), factory.identifier().build()),
                willThrow(FileTransferException.class, "No files to upload")
        );
    }

    @Test
    void uploadNonReadableFiles() throws IOException {
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        File file1 = randomFile(sourceDir, chunkSize - 1);
        File file2 = randomFile(sourceDir, chunkSize + 1);
        File file3 = randomFile(sourceDir, chunkSize * 2);

        assertTrue(file2.setReadable(false));

        sourceNode.fileTransferringService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        Node targetNode = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferringService().addFileConsumer(Identifier.class, ((metadata, uploadedFiles) -> {
            uploadedFilesFuture.complete(uploadedFiles);
            return completedFuture(null);
        }));

        CompletableFuture<Void> upload = sourceNode.fileTransferringService().upload(targetNode.nodeName(), factory.identifier().build());
        assertThat(
                upload,
                willThrow(FileTransferException.class, "Failed to create a file transfer stream")
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
                Identifier.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        Node targetNode = cluster.members.get(1);

        CompletableFuture<List<File>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferringService().addFileConsumer(Identifier.class, ((metadata, uploadedFiles) -> {
            uploadedFilesFuture.complete(uploadedFiles);
            return completedFuture(null);
        }));

        assertThat(
                sourceNode.fileTransferringService().upload(targetNode.nodeName(), factory.identifier().build()),
                willThrow(RuntimeException.class, "Test exception")
        );

        assertFalse(uploadedFilesFuture.isDone());
    }
}
