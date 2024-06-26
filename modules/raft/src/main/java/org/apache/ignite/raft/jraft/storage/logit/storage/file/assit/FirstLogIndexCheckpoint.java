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

import java.nio.ByteBuffer;
import org.apache.ignite.raft.jraft.option.RaftOptions;

/**
 * This checkpoint is used for save firstLogIndex
 */
public class FirstLogIndexCheckpoint extends Checkpoint {

    // LogStorage first log index
    public long firstLogIndex = -1;

    public FirstLogIndexCheckpoint(final String path, RaftOptions raftOptions) {
        super(path, raftOptions);
    }

    /**
     * firstLogIndex (8 bytes)
     */
    @Override
    public ByteBuffer encode() {
        return ByteBuffer.allocate(8).putLong(this.firstLogIndex);
    }

    @Override
    public boolean decode(final ByteBuffer buf) {
        if (buf.remaining() < 8) {
            return false;
        }
        this.firstLogIndex = buf.getLong();
        return this.firstLogIndex >= 0;
    }

    public void setFirstLogIndex(final long firstLogIndex) {
        this.firstLogIndex = firstLogIndex;
    }

    public void reset() {
        this.firstLogIndex = -1;
    }

    public boolean isInit() {
        return this.firstLogIndex >= 0;
    }

    @Override
    public String toString() {
        return "FirstLogIndexCheckpoint{" + "firstLogIndex=" + firstLogIndex + '}';
    }
}
