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
package org.apache.ignite.raft.jraft.entity.codec;

import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Decoder;
import org.apache.ignite.raft.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v2.V2Decoder;

/**
 * Decoder that supports both V1 and V2 log entry codec protocol.
 * Uses magic byte to auto-detect the version.
 */
public class AutoDetectDecoder implements LogEntryDecoder {

    private AutoDetectDecoder() {

    }

    public static final AutoDetectDecoder INSTANCE = new AutoDetectDecoder();

    @Override
    public LogEntry decode(final byte[] bs) {
        if (bs == null || bs.length < 1) {
            return null;
        }

        // Auto-detect version based on magic byte
        if (bs[0] == LogEntryV2CodecFactory.MAGIC) {
            return V2Decoder.INSTANCE.decode(bs);
        } else if (bs[0] == LogEntryV1CodecFactory.MAGIC) {
            return V1Decoder.INSTANCE.decode(bs);
        } else {
            // Unknown magic byte - corrupted data
            return null;
        }
    }

}
