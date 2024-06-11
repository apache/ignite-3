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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.apache.ignite.internal.network.file.PathAssertions.namesAndContentEquals;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.configuration.FileTransferConfiguration;
import org.apache.ignite.internal.network.file.TestCluster.Node;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.Identifier;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for file transferring.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
@Disabled("https://issues.apache.org/jira/browse/IGNITE-22438")
public class ItFileTransferTest extends BaseIgniteAbstractTest {
    private static final String DOWNLOADS_DIR = "downloads";

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration("mock.chunkSize=1024")
    private FileTransferConfiguration configuration;

    private TestCluster cluster;

    private Path sourceDir;

    private final FileTransferFactory messageFactory = new FileTransferFactory();

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
    void downloadToEmptyDir() {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, chunkSize);
        Path file2 = randomFile(sourceDir, chunkSize + 1);
        Path file3 = randomFile(sourceDir, chunkSize * 2);
        Path file4 = randomFile(sourceDir, chunkSize * 2);

        // Add file provider to the source node.
        List<Path> files = List.of(file1, file2, file3, file4);
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(files)
        );

        // Download files on the target node from the source node.
        Node targetNode = cluster.members.get(1);
        CompletableFuture<List<Path>> downloadedFilesFuture = targetNode.fileTransferService().download(
                sourceNode.nodeName(),
                messageFactory.identifier().build(),
                targetNode.workDir().resolve(DOWNLOADS_DIR)
        );

        // Check that files were downloaded.
        assertThat(
                downloadedFilesFuture,
                willBe(namesAndContentEquals(files))
        );

        // Check that files were downloaded to the correct directory.
        assertDownloadsDirContainsFiles(targetNode, files);

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    void downloadToNonEmptyDir() throws IOException {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, chunkSize);
        Path file2 = randomFile(sourceDir, chunkSize + 1);
        Path file3 = randomFile(sourceDir, chunkSize * 2);
        Path file4 = randomFile(sourceDir, chunkSize * 2);

        // Add file provider to the source node.
        List<Path> files = List.of(file1, file2, file3, file4);
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(files)
        );

        Node targetNode = cluster.members.get(1);

        // Create downloads directory.
        Path targetNodeDownloadsDir = targetNode.workDir().resolve(DOWNLOADS_DIR);
        Files.createDirectories(targetNodeDownloadsDir);

        // Create some files in the downloads directory.
        Path file5 = randomFile(targetNodeDownloadsDir, chunkSize);
        Path file6 = randomFile(targetNodeDownloadsDir, chunkSize + 1);

        // Download files on the target node from the source node.
        CompletableFuture<List<Path>> downloadedFilesFuture = targetNode.fileTransferService().download(
                sourceNode.nodeName(),
                messageFactory.identifier().build(),
                targetNodeDownloadsDir
        );

        // Check that files were downloaded.
        assertThat(
                downloadedFilesFuture,
                willBe(namesAndContentEquals(files))
        );

        // Check that files were downloaded to the correct directory.
        assertDownloadsDirContainsFiles(targetNode, files);

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);

        // Check that previous files in the downloads directory were deleted.
        try (Stream<Path> stream = Files.list(targetNodeDownloadsDir)) {
            List<String> filesInDownloadsDir = stream.map(Path::getFileName)
                    .map(Path::toString)
                    .collect(toList());
            assertThat(filesInDownloadsDir, not(containsInAnyOrder(file5.getFileName().toString(), file6.getFileName().toString())));
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS) // Windows doesn't support posix file permissions.
    void downloadNonReadableFiles() {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, chunkSize - 1);
        Path file2 = randomFile(sourceDir, chunkSize + 1);
        Path file3 = randomFile(sourceDir, chunkSize * 2);

        // Make file2 non-readable.
        assertTrue(file2.toFile().setReadable(false));

        // Add file provider to the source node.
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        // Download files on the target node from the source node.
        Node targetNode = cluster.members.get(1);
        CompletableFuture<List<Path>> download = targetNode.fileTransferService().download(
                sourceNode.nodeName(),
                messageFactory.identifier().build(),
                targetNode.workDir().resolve(DOWNLOADS_DIR)
        );

        // Check that files transfer failed.
        assertThat(download, willThrow(FileTransferException.class, "Failed to create a file transfer stream"));

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    void downloadFilesWhenProviderThrowsException() {
        // Add file provider to the source node that throws an exception.
        Node sourceNode = cluster.members.get(0);
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        // Download files on the target node from the source node.
        Node targetNode = cluster.members.get(1);
        CompletableFuture<List<Path>> download = targetNode.fileTransferService().download(
                sourceNode.nodeName(),
                messageFactory.identifier().build(),
                targetNode.workDir().resolve(DOWNLOADS_DIR)
        );

        // Check that files transfer failed.
        assertThat(download, willThrowWithCauseOrSuppressed(FileTransferException.class));

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS) // Windows doesn't support posix file permissions.
    void downloadFilesWhenDoNotHaveAccessToWrite() {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, 0);
        Path file2 = randomFile(sourceDir, chunkSize + 1);
        Path file3 = randomFile(sourceDir, chunkSize * 2);
        Path file4 = randomFile(sourceDir, chunkSize * 2);

        // Add file provider to the source node.
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3, file4))
        );

        // Make work directory not writable on the target node.
        Node targetNode = cluster.members.get(1);
        assertTrue(targetNode.workDir().toFile().setWritable(false));

        // Download files on the target node from the source node.
        CompletableFuture<List<Path>> downloadedFilesFuture = targetNode.fileTransferService().download(
                sourceNode.nodeName(),
                messageFactory.identifier().build(),
                targetNode.workDir().resolve(DOWNLOADS_DIR)
        );

        // Check that files transfer failed.
        assertThat(
                downloadedFilesFuture,
                willThrowWithCauseOrSuppressed(AccessDeniedException.class)
        );

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    void downloadEmptyFileList() {
        // Set empty file list provider on the source node.
        Node sourceNode = cluster.members.get(0);
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> emptyListCompletedFuture()
        );

        // Download files on the target node from the source node.
        Node targetNode = cluster.members.get(1);
        CompletableFuture<List<Path>> downloadedFiles = targetNode.fileTransferService().download(
                sourceNode.nodeName(),
                messageFactory.identifier().build(),
                targetNode.workDir().resolve(DOWNLOADS_DIR)

        );

        // Check that files transfer failed.
        assertThat(downloadedFiles, willThrow(FileTransferException.class, "No files to download"));

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    void upload() {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, chunkSize);
        Path file2 = randomFile(sourceDir, chunkSize - 1);
        Path file3 = randomFile(sourceDir, chunkSize + 1);
        Path file4 = randomFile(sourceDir, chunkSize * 2);

        // Add file provider to the source node.
        List<Path> files = List.of(file1, file2, file3, file4);
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(files)
        );

        Node targetNode = cluster.members.get(1);

        // The future that will be completed when the consumer is called.
        CompletableFuture<Void> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferService().addFileConsumer(Identifier.class, ((identifier, uploadedFiles) -> {
            return uploadedFilesFuture.completeAsync(() -> {
                assertThat(uploadedFiles, namesAndContentEquals(files));
                return null;
            });
        }));

        // Upload files to the target node from the source node.
        CompletableFuture<Void> upload = sourceNode.fileTransferService()
                .upload(targetNode.nodeName(), messageFactory.identifier().build());

        // Check that transfer was successful.
        assertThat(
                upload,
                CompletableFutureMatcher.willCompleteSuccessfully()
        );

        // Check the consumer was called.
        assertThat(
                uploadedFilesFuture,
                CompletableFutureMatcher.willCompleteSuccessfully()
        );

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    void uploadEmptyFileList() {
        // Set empty file list provider on the source node.
        Node sourceNode = cluster.members.get(0);
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> emptyListCompletedFuture()
        );

        // Upload files to the target node from the source node.
        Node targetNode = cluster.members.get(1);
        CompletableFuture<Void> uploaded = sourceNode.fileTransferService()
                .upload(targetNode.nodeName(), messageFactory.identifier().build());

        // Check that files transfer failed.
        assertThat(
                uploaded,
                willThrow(FileTransferException.class, "No files to upload")
        );

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS) // Windows doesn't support posix file permissions.
    void uploadNonReadableFiles() {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, chunkSize - 1);
        Path file2 = randomFile(sourceDir, chunkSize + 1);
        Path file3 = randomFile(sourceDir, chunkSize * 2);

        // Make file2 non-readable.
        assertTrue(file2.toFile().setReadable(false));

        // Add file provider to the source node.
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3))
        );

        // Set file consumer on the target node.
        Node targetNode = cluster.members.get(1);

        // The future will be completed when the file consumer is called.
        CompletableFuture<List<Path>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferService().addFileConsumer(Identifier.class, ((identifier, uploadedFiles) -> {
            uploadedFilesFuture.complete(uploadedFiles);
            return nullCompletedFuture();
        }));

        // Upload files to the target node from the source node.
        Identifier identifier = messageFactory.identifier().build();
        CompletableFuture<Void> upload = sourceNode.fileTransferService().upload(targetNode.nodeName(), identifier);

        // Check that files transfer failed.
        assertThat(
                upload,
                willThrow(FileTransferException.class, "Failed to create a file transfer stream")
        );

        // Check the consumer was not called.
        assertFalse(uploadedFilesFuture.isDone());

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    void uploadFilesWhenProviderThrowsException() {
        // Set file provider on the source node that throws an exception.
        Node sourceNode = cluster.members.get(0);

        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> failedFuture(new RuntimeException("Test exception"))
        );

        // Set file consumer on the target node.
        Node targetNode = cluster.members.get(1);

        // The future will be completed when the file consumer is called.
        CompletableFuture<List<Path>> uploadedFilesFuture = new CompletableFuture<>();
        targetNode.fileTransferService().addFileConsumer(Identifier.class, ((identifier, uploadedFiles) -> {
            uploadedFilesFuture.complete(uploadedFiles);
            return nullCompletedFuture();
        }));

        // Upload files to the target node from the source node.
        CompletableFuture<Void> uploaded = sourceNode.fileTransferService()
                .upload(targetNode.nodeName(), messageFactory.identifier().build());

        // Check that files transfer failed.
        assertThat(
                uploaded,
                willThrow(RuntimeException.class, "Test exception")
        );

        // Check the consumer was not called.
        assertFalse(uploadedFilesFuture.isDone());

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS) // Windows doesn't support posix file permissions.
    void uploadFilesWhenDoNotHaveAccessToWrite() {
        // Generate files on the source node.
        Node sourceNode = cluster.members.get(0);

        int chunkSize = configuration.value().chunkSize();
        Path file1 = randomFile(sourceDir, 0);
        Path file2 = randomFile(sourceDir, chunkSize + 1);
        Path file3 = randomFile(sourceDir, chunkSize * 2);
        Path file4 = randomFile(sourceDir, chunkSize * 2);

        // Add file provider to the source node.
        sourceNode.fileTransferService().addFileProvider(
                Identifier.class,
                req -> completedFuture(List.of(file1, file2, file3, file4))
        );

        // Make work directory not writable on the target node.
        Node targetNode = cluster.members.get(1);
        assertTrue(targetNode.workDir().toFile().setWritable(false));

        // Upload files to the target node from the source node.
        CompletableFuture<Void> uploaded = sourceNode.fileTransferService().upload(
                targetNode.nodeName(), messageFactory.identifier().build()
        );

        // Check that files transfer failed.
        assertThat(uploaded, willThrow(FileTransferException.class, "Failed to upload files:"));

        // Check temporary files were deleted.
        assertTemporaryFilesWereDeleted(targetNode);
    }

    private static void assertTemporaryFilesWereDeleted(Node targetNode) {
        await().until(() -> {
            try (Stream<Path> stream = Files.list(targetNode.workDir())) {
                return stream.map(Path::getFileName)
                        .map(Path::toString)
                        .filter(path -> !path.equals(DOWNLOADS_DIR))
                        .collect(toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, emptyIterable());
    }

    private static void assertDownloadsDirContainsFiles(Node node, List<Path> expectedFiles) {
        try (Stream<Path> stream = Files.list(node.workDir().resolve(DOWNLOADS_DIR))) {
            assertThat(stream.collect(toList()), namesAndContentEquals(expectedFiles));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
