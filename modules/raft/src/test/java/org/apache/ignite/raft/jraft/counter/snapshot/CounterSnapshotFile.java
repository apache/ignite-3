/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.counter.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counter snapshot file.
 */
public class CounterSnapshotFile {
    private static final Logger LOG = LoggerFactory.getLogger(CounterSnapshotFile.class);

    private String path;

    public CounterSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    /**
     * Save value to snapshot file.
     */
    public void save(final long value) throws IOException{
        try {
            Files.writeString(new File(path).toPath(), String.valueOf(value));
        }
        catch (IOException e) {
            LOG.error("Fail to save snapshot", e);

            throw e;
        }
    }

    public long load() throws IOException {
        final String s = Files.readString(new File(path).toPath());
        if (!StringUtils.isBlank(s)) {
            return Long.parseLong(s);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + s);
    }
}
