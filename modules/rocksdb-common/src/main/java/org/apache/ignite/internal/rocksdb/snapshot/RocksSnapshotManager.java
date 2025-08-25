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

package org.apache.ignite.internal.rocksdb.snapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileWriter;

/**
 * Class for creating and restoring RocksDB snapshots.
 */
public class RocksSnapshotManager {
    /** Suffix for the temporary snapshot folder. */
    private static final String TMP_SUFFIX = ".tmp";

    private final RocksDB db;

    private final Collection<ColumnFamilyRange> ranges;

    private final Executor executor;

    /**
     * Creates a new instance of the snapshot manager.
     * This instance <b>does not</b> own any of the provided resources and will not close them.
     *
     * @param db RocksDB instance which snapshots will be managed.
     * @param ranges Key ranges of Column Families that exist in the provided {@code db} instance.
     * @param executor Executor which will be used for creating snapshots.
     */
    public RocksSnapshotManager(RocksDB db, Collection<ColumnFamilyRange> ranges, Executor executor) {
        assert !ranges.isEmpty();

        this.db = db;
        this.ranges = List.copyOf(ranges);
        this.executor = executor;
    }

    /**
     * Creates a snapshot of the enclosed RocksDB instance and saves it into a provided folder.
     *
     * @param snapshotDir Folder to save the snapshot into.
     * @return Future that either completes successfully upon snapshot creation or signals a failure.
     */
    public CompletableFuture<Void> createSnapshot(Path snapshotDir) {
        Path tmpPath = Paths.get(snapshotDir.toString() + TMP_SUFFIX);

        // The snapshot reference must be taken synchronously, otherwise we might let more writes sneak into the snapshot than needed.
        Snapshot snapshot = db.getSnapshot();

        return CompletableFuture.supplyAsync(
                () -> {
                    createTmpSnapshotDir(tmpPath);

                    // Create futures for capturing SST snapshots of the column families
                    CompletableFuture<?>[] sstFutures = ranges.stream()
                            .map(cf -> createSstFileAsync(cf, snapshot, tmpPath))
                            .toArray(CompletableFuture[]::new);

                    return CompletableFuture.allOf(sstFutures);
                }, executor)
                .thenCompose(Function.identity())
                .whenCompleteAsync((ignored, e) -> {
                    db.releaseSnapshot(snapshot);

                    // Snapshot is not actually closed here, because a Snapshot instance doesn't own a pointer, the
                    // database does. Calling close to maintain the AutoCloseable semantics
                    snapshot.close();

                    if (e != null) {
                        return;
                    }

                    // Delete snapshot directory if it already exists
                    IgniteUtils.deleteIfExists(snapshotDir);

                    try {
                        // Rename the temporary directory
                        IgniteUtils.atomicMoveFile(tmpPath, snapshotDir, null);
                    } catch (IOException ex) {
                        throw new IgniteInternalException("Failed to rename: " + tmpPath + " to " + snapshotDir, ex);
                    }
                }, executor)
                .thenApply(v -> null);
    }

    /**
     * Creates a temporary directory for storing intermediate results while creating a snapshot.
     *
     * @param tmpDirPath Path to the temporary directory.
     */
    private static void createTmpSnapshotDir(Path tmpDirPath) {
        IgniteUtils.deleteIfExists(tmpDirPath);

        try {
            Files.createDirectories(tmpDirPath);
        } catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory: " + tmpDirPath, e);
        }
    }

    /**
     * Creates an SST file for the column family (async version).
     *
     * @param range Column family range.
     * @param snapshot Point-in-time snapshot.
     * @param snapshotDir Directory to put the SST file in.
     */
    private CompletableFuture<Void> createSstFileAsync(ColumnFamilyRange range, Snapshot snapshot, Path snapshotDir) {
        return CompletableFuture.runAsync(() -> createSstFile(range, snapshot, snapshotDir), executor);
    }

    /**
     * Creates an SST file for the column family.
     *
     * @param range Column family range.
     * @param snapshot Point-in-time snapshot.
     * @param snapshotDir Directory to put the SST file in.
     */
    private void createSstFile(ColumnFamilyRange range, Snapshot snapshot, Path snapshotDir) {
        try (
                EnvOptions envOptions = new EnvOptions();
                Options options = new Options().setEnv(db.getEnv());
                SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options);
                ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                RocksIterator it = rangeIterator(range, readOptions)
        ) {
            Path sstFile = snapshotDir.resolve(range.columnFamily().name());

            sstFileWriter.open(sstFile.toString());

            RocksUtils.forEach(it, sstFileWriter::put);

            sstFileWriter.finish();
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Failed to write snapshot", e);
        }
    }

    /**
     * Creates an iterator over the provided key range.
     */
    private static RocksIterator rangeIterator(ColumnFamilyRange range, ReadOptions options) {
        if (range.isFullRange()) {
            RocksIterator it = range.columnFamily().newIterator(options);

            it.seekToFirst();

            return it;
        } else {
            options.setIterateUpperBound(new Slice(range.upperBound()));

            RocksIterator it = range.columnFamily().newIterator(options);

            it.seek(range.lowerBound());

            return it;
        }
    }

    /**
     * Restores the snapshot that was created by {@link #createSnapshot}.
     *
     * <p>This method loads the snapshot as-is, overwriting the existing keys if necessary. Most of the times storage implementations
     * should manually remove all data before restoring a snapshot.
     *
     * @param snapshotDir Path to the directory where a snapshot was created.
     */
    public void restoreSnapshot(Path snapshotDir) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-23393 No failure protection if we fail during one of ingestions.
        try (IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()) {
            for (ColumnFamilyRange range : ranges) {
                Path snapshotPath = snapshotDir.resolve(range.columnFamily().name());

                if (!Files.exists(snapshotPath)) {
                    throw new IgniteInternalException("Snapshot not found: " + snapshotPath);
                }

                range.columnFamily().ingestExternalFile(List.of(snapshotPath.toString()), ingestOptions);
            }
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Fail to ingest sst file at path: " + snapshotDir, e);
        }
    }
}
