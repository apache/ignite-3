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

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;
import static org.apache.ignite.internal.util.IgniteUtils.fsyncFile;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Represents an index file create by an {@link IndexFileManager}.
 *
 * <p>Not thread-safe.
 */
class IndexFile {
    private static final IgniteLogger LOG = Loggers.forClass(IndexFile.class);

    private final String name;

    private Path path;

    IndexFile(String name, Path path) {
        this.name = name;
        this.path = path;
    }

    void syncAndRename() throws IOException {
        fsyncFile(path);

        path = atomicMoveFile(path, path.getParent().resolve(name), LOG);
    }

    /** Returns the name of the index file. */
    String name() {
        return name;
    }

    /**
     * Returns the current path to the index file.
     *
     * <p>The file is originally created as a temporary file until it gets renamed by calling {@link #syncAndRename}.
     */
    Path path() {
        return path;
    }
}
