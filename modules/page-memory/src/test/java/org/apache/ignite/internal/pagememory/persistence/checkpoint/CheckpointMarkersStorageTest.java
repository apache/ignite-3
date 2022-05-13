/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.list;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.IgniteUtils.deleteIfExists;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link CheckpointMarkersStorage} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
public class CheckpointMarkersStorageTest {
    @WorkDirectory
    private Path workDir;

    @Test
    void testFailCreateCheckpointDir() throws Exception {
        Path testFile = createFile(workDir.resolve("testFile"));

        try (FileWriter fileWriter = new FileWriter(testFile.toFile(), StandardCharsets.UTF_8)) {
            fileWriter.write("testString");

            fileWriter.flush();
        }

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> new CheckpointMarkersStorage(testFile)
        );

        assertThat(exception.getMessage(), startsWith("Could not create directory for checkpoint metadata"));
    }

    @Test
    void testNotOnlyMarkersFiles() throws Exception {
        createFile(createDirectories(cpDir()).resolve("test"));

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> new CheckpointMarkersStorage(workDir)
        );

        assertThat(exception.getMessage(), startsWith("Not checkpoint markers found, they need to be removed manually"));
    }

    @Test
    void testTmpMarkersFiles() throws Exception {
        createDirectories(cpDir());

        UUID id = UUID.randomUUID();

        createFile(cpDir().resolve(startMarkerFileName(id) + ".tmp"));
        createFile(cpDir().resolve(endMarkerFileName(id) + ".tmp"));

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> new CheckpointMarkersStorage(workDir)
        );

        assertThat(exception.getMessage(), startsWith("Not checkpoint markers found, they need to be removed manually"));
    }

    @Test
    void testCheckpointWithoutEndMarker() throws Exception {
        createDirectories(cpDir());

        createFile(startMarkerFilePath(UUID.randomUUID()));

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> new CheckpointMarkersStorage(workDir)
        );

        assertThat(exception.getMessage(), startsWith("Found incomplete checkpoints"));
    }

    @Test
    void testCreateMarkers() throws Exception {
        UUID id0 = UUID.randomUUID();

        CheckpointMarkersStorage markersStorage = new CheckpointMarkersStorage(workDir);

        markersStorage.onCheckpointBegin(id0);
        markersStorage.onCheckpointEnd(id0);

        assertThat(
                list(cpDir()).collect(toSet()),
                equalTo(Set.of(startMarkerFilePath(id0), endMarkerFilePath(id0)))
        );

        deleteIfExists(cpDir());

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> markersStorage.onCheckpointBegin(UUID.randomUUID())
        );

        assertThat(exception.getMessage(), startsWith("Could not create start checkpoint marker"));

        exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> markersStorage.onCheckpointEnd(id0)
        );

        assertThat(exception.getMessage(), startsWith("Could not create end checkpoint marker"));
    }

    @Test
    void testCleanupMarkers() throws Exception {
        CheckpointMarkersStorage markersStorage = new CheckpointMarkersStorage(workDir);

        UUID id0 = UUID.randomUUID();
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        markersStorage.onCheckpointBegin(id0);
        markersStorage.onCheckpointEnd(id0);

        markersStorage.onCheckpointBegin(id1);
        markersStorage.onCheckpointEnd(id1);

        markersStorage.onCheckpointBegin(id2);
        markersStorage.onCheckpointEnd(id2);

        assertThat(
                list(cpDir()).collect(toSet()),
                equalTo(Set.of(startMarkerFilePath(id2), endMarkerFilePath(id2)))
        );
    }

    private Path cpDir() {
        return workDir.resolve("cp");
    }

    private Path startMarkerFilePath(UUID id) {
        return cpDir().resolve(startMarkerFileName(id));
    }

    private Path endMarkerFilePath(UUID id) {
        return cpDir().resolve(endMarkerFileName(id));
    }

    private static String startMarkerFileName(UUID id) {
        return id + "-START.bin";
    }

    private static String endMarkerFileName(UUID id) {
        return id + "-END.bin";
    }
}
