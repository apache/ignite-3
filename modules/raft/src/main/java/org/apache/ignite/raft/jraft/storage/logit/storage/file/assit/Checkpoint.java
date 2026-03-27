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

package org.apache.ignite.raft.jraft.storage.logit.storage.file.assit;

import java.io.IOException;

import java.nio.file.Path;

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.io.MessageFile;

/**
 * Abstract checkpoint file
 */
public abstract class Checkpoint {

    private final String path;
    private final RaftMessagesFactory raftMessagesFactory;

    public Checkpoint(final String path, RaftOptions raftOptions) {
        super();
        this.path = path;
        this.raftMessagesFactory = raftOptions.getRaftMessagesFactory();
    }

    /**
     * Encode metadata
     */
    public abstract byte[] encode();

    /**
     * Decode file data
     */
    public abstract boolean decode(final byte[] bs);

    public synchronized boolean save() throws IOException {
        MessageFile file = new MessageFile(this.path);
        final byte[] data = this.encode();
        final LocalFileMeta meta = raftMessagesFactory.localFileMeta() //
            .userMeta(data) //
            .build();
        return file.save(meta, true);
    }

    public void load() throws IOException {
        MessageFile file = new MessageFile(this.path);
        final LocalFileMeta meta = file.load();
        if (meta != null) {
            final byte[] data = meta.userMeta();
            decode(data);
        }
    }

    public void destroy() {
        IgniteUtils.deleteIfExists(Path.of(this.path));
    }

    public String getPath() {
        return this.path;
    }

}
