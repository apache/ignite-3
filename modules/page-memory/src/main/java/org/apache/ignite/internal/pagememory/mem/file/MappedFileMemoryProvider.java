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

package org.apache.ignite.internal.pagememory.mem.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Memory provider implementation based on memory mapped file.
 *
 * <p>Doesn't support memory reuse semantics.
 */
public class MappedFileMemoryProvider implements DirectMemoryProvider {
    private static final String ALLOCATOR_FILE_PREFIX = "allocator-";

    private static final FilenameFilter ALLOCATOR_FILTER = (dir, name) -> name.startsWith(ALLOCATOR_FILE_PREFIX);

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(MappedFileMemoryProvider.class);

    /** File allocation path. */
    private final File allocationPath;

    private long[] sizes;

    private List<MappedFile> mappedFiles;

    /** Flag shows if current memory provider have been already initialized. */
    private boolean isInit;

    /**
     * Constructor.
     *
     * @param allocationPath Allocation path.
     */
    public MappedFileMemoryProvider(File allocationPath) {
        this.allocationPath = allocationPath;
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(long[] sizes) {
        if (isInit) {
            throw new IgniteInternalException("Second initialization does not allowed for current provider");
        }

        this.sizes = sizes;

        mappedFiles = new ArrayList<>(sizes.length);

        if (!allocationPath.exists()) {
            if (!allocationPath.mkdirs()) {
                throw new IgniteInternalException("Failed to initialize allocation path (make sure directory is "
                        + "writable for the current user): " + allocationPath);
            }
        }

        if (!allocationPath.isDirectory()) {
            throw new IgniteInternalException("Failed to initialize allocation path (path is a file): " + allocationPath);
        }

        File[] files = allocationPath.listFiles(ALLOCATOR_FILTER);

        if (files.length != 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Will clean up the following files upon start: " + Arrays.asList(files));
            }

            for (File file : files) {
                if (!file.delete()) {
                    throw new IgniteInternalException("Failed to delete allocated file on start (make sure file is not "
                            + "opened by another process and current user has enough rights): " + file);
                }
            }
        }

        isInit = true;
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown(boolean deallocate) {
        if (mappedFiles != null) {
            for (MappedFile file : mappedFiles) {
                try {
                    file.close();
                } catch (IOException e) {
                    LOG.error("Failed to close memory-mapped file upon stop (will ignore) [file="
                            + file.file() + ", err=" + e.getMessage() + ']');
                }
            }

            mappedFiles = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public DirectMemoryRegion nextRegion() {
        try {
            if (mappedFiles.size() == sizes.length) {
                return null;
            }

            int idx = mappedFiles.size();

            long chunkSize = sizes[idx];

            File file = new File(allocationPath, ALLOCATOR_FILE_PREFIX + alignInt(idx));

            MappedFile mappedFile = new MappedFile(file, chunkSize);

            mappedFiles.add(mappedFile);

            return mappedFile;
        } catch (IOException e) {
            LOG.error("Failed to allocate next memory-mapped region", e);

            return null;
        }
    }

    /**
     * Returns 0-aligned string.
     *
     * @param idx Index.
     */
    private static String alignInt(int idx) {
        String idxStr = String.valueOf(idx);

        StringBuilder res = new StringBuilder();

        for (int i = 0; i < 8 - idxStr.length(); i++) {
            res.append('0');
        }

        res.append(idxStr);

        return res.toString();
    }
}
