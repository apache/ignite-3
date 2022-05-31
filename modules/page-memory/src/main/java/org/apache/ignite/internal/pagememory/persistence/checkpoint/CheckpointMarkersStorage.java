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
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.list;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Abstraction responsible for managing checkpoint markers storage.
 */
// TODO: IGNITE-15818 At the moment, a simple implementation has been made, it may need to be redone, you need to check it
public class CheckpointMarkersStorage {
    /** Checkpoint start marker. */
    private static final String CHECKPOINT_START_MARKER = "START";

    /** Checkpoint end marker. */
    private static final String CHECKPOINT_END_MARKER = "END";

    /** Checkpoint marker file name pattern. */
    private static final Pattern CHECKPOINT_MARKER_FILE_NAME_PATTERN = Pattern.compile("(.*)-(START|END)\\.bin");

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end markers. */
    private final Path checkpointDir;

    /** Checkpoint IDs. */
    private final Set<UUID> checkpointIds;

    /**
     * Constructor.
     *
     * @param storagePath Storage path.
     * @throws IgniteInternalCheckedException If failed.
     */
    public CheckpointMarkersStorage(
            Path storagePath
    ) throws IgniteInternalCheckedException {
        checkpointDir = storagePath.resolve("cp");

        try {
            createDirectories(checkpointDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create directory for checkpoint metadata: " + checkpointDir, e);
        }

        checkCheckpointDir(checkpointDir);

        try {
            checkpointIds = list(checkpointDir)
                    .map(CheckpointMarkersStorage::parseCheckpointIdFromMarkerFile)
                    .collect(toCollection(ConcurrentHashMap::newKeySet));
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not read checkpoint markers: " + checkpointDir, e);
        }
    }

    /**
     * Callback at the start of the checkpoint.
     *
     * <p>Creates a start marker for a checkpoint.
     *
     * @param checkpointId Checkpoint id.
     */
    public void onCheckpointBegin(UUID checkpointId) throws IgniteInternalCheckedException {
        assert !checkpointIds.contains(checkpointId) : checkpointId;

        Path checkpointStartMarker = checkpointDir.resolve(checkpointMarkerFileName(checkpointId, CHECKPOINT_START_MARKER));

        try {
            createFile(checkpointStartMarker);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create start checkpoint marker: " + checkpointStartMarker, e);
        }

        checkpointIds.add(checkpointId);
    }

    /**
     * Callback at the end of the checkpoint.
     *
     * <p>Creates an end marker for a checkpoint.
     *
     * @param checkpointId Checkpoint id.
     */
    public void onCheckpointEnd(UUID checkpointId) throws IgniteInternalCheckedException {
        assert checkpointIds.contains(checkpointId) : checkpointId;

        Path checkpointEndMarker = checkpointDir.resolve(checkpointMarkerFileName(checkpointId, CHECKPOINT_END_MARKER));

        try {
            createFile(checkpointEndMarker);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create end checkpoint marker: " + checkpointEndMarker, e);
        }

        for (Iterator<UUID> it = checkpointIds.iterator(); it.hasNext(); ) {
            UUID id = it.next();

            if (!id.equals(checkpointId)) {
                removeCheckpointMarkers(id);

                it.remove();
            }
        }
    }

    private void removeCheckpointMarkers(UUID checkpointId) {
        Path startMarker = checkpointDir.resolve(checkpointMarkerFileName(checkpointId, CHECKPOINT_START_MARKER));
        Path endMarker = checkpointDir.resolve(checkpointMarkerFileName(checkpointId, CHECKPOINT_END_MARKER));

        if (exists(startMarker)) {
            startMarker.toFile().delete();
        }

        if (exists(endMarker)) {
            endMarker.toFile().delete();
        }
    }

    /**
     * Checks that the directory contains only paired (start and end) checkpoint markers.
     */
    private static void checkCheckpointDir(Path checkpointDir) throws IgniteInternalCheckedException {
        assert isDirectory(checkpointDir) : checkpointDir;

        try {
            List<Path> notCheckpointMarkers = list(checkpointDir)
                    .filter(path -> parseCheckpointIdFromMarkerFile(path) == null)
                    .collect(toList());

            if (!notCheckpointMarkers.isEmpty()) {
                throw new IgniteInternalCheckedException(
                        "Not checkpoint markers found, they need to be removed manually: " + notCheckpointMarkers
                );
            }

            Map<UUID, List<Path>> checkpointMarkers = list(checkpointDir)
                    .collect(groupingBy(CheckpointMarkersStorage::parseCheckpointIdFromMarkerFile));

            List<UUID> checkpointsWithoutEndMarker = checkpointMarkers.entrySet().stream()
                    .filter(e -> e.getValue().stream().noneMatch(path -> path.getFileName().toString().contains(CHECKPOINT_END_MARKER)))
                    .map(Entry::getKey)
                    .collect(toList());

            if (!checkpointsWithoutEndMarker.isEmpty()) {
                throw new IgniteInternalCheckedException("Found incomplete checkpoints: " + checkpointsWithoutEndMarker);
            }
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not reads checkpoint markers: " + checkpointDir, e);
        }
    }

    private static String checkpointMarkerFileName(UUID id, String checkpointMarker) {
        return id + "-" + checkpointMarker + ".bin";
    }

    private static @Nullable UUID parseCheckpointIdFromMarkerFile(Path checkpointMarkerPath) {
        Matcher matcher = CHECKPOINT_MARKER_FILE_NAME_PATTERN.matcher(checkpointMarkerPath.getFileName().toString());

        if (!matcher.matches()) {
            return null;
        }

        return UUID.fromString(matcher.group(1));
    }
}
