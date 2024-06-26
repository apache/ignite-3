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
import org.apache.ignite.raft.jraft.util.AsciiStringUtil;

/**
 * This checkpoint is used for save flushPosition and last LogIndex
 */
public class FlushStatusCheckpoint extends Checkpoint {

    // Current file name
    public String fileName;
    // Current flush position.
    public long   flushPosition;
    // Last logIndex
    public long   lastLogIndex;

    public FlushStatusCheckpoint(final String path, RaftOptions raftOptions) {
        super(path, raftOptions);
    }

    /**
     * flushPosition (8 bytes) + lastLogIndex (8 bytes) + path(string bytes)
     */
    @Override
    public ByteBuffer encode() {
        byte[] ps = AsciiStringUtil.unsafeEncode(this.fileName);
        return ByteBuffer.allocate(16 + ps.length)
            .putLong(this.flushPosition)
            .putLong(this.lastLogIndex)
            .put(ByteBuffer.wrap(ps));
    }

    @Override
    public boolean decode(final ByteBuffer buf) {
        if (buf.capacity() < 16) {
            return false;
        }
        this.flushPosition = buf.getLong();
        this.lastLogIndex = buf.getLong();
        this.fileName = AsciiStringUtil.unsafeDecode(buf);
        return this.flushPosition >= 0 && this.lastLogIndex >= 0 && !this.fileName.isEmpty();
    }

    public void setFileName(final String fileName) {
        this.fileName = fileName;
    }

    public void setFlushPosition(final long flushPosition) {
        this.flushPosition = flushPosition;
    }

    public void setLastLogIndex(final long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    @Override
    public String toString() {
        return "FlushStatusCheckpoint{" + "segFilename='" + fileName + '\'' + ", flushPosition=" + flushPosition
               + ", lastLogIndex=" + lastLogIndex + '}';
    }
}
