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
package org.apache.ignite.raft.jraft.entity.codec.v2;

import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;

/**
 * Log entry codec implementation V2. Extends V1 format with sequence token support.
 * Data format description:
 * <ul>
 *     <li>Magic header, 1 byte. {@link #MAGIC}</li>
 *     <li>Log entry type, formally a var-long, effectively 1 byte. {@link EntryType}</li>
 *     <li>Log index, var-long, from 1 to 9 bytes.</li>
 *     <li>Log term, var-long, from 1 to 9 bytes.</li>
 *     <li>Checksum, 8 bytes, Little Endian.</li>
 *     <li><b>Sequence token, var-long, from 1 to 9 bytes. (NEW in V2)</b></li>
 *     <li><b>Old sequence token, var-long, from 1 to 9 bytes. (NEW in V2)</b></li>
 *     <li>If type is not {@link EntryType#ENTRY_TYPE_DATA}:<ul>
 *         <li>Number of peers, var-long. Following it is the array of peers:</li>
 *         <li>Length of the peer name, 2 bytes, Little Endian.</li>
 *         <li>ASCII characters of the peer name, according to the read count.</li>
 *         <li>Peer index, var-long.</li>
 *         <li>Peer priority + 1, var-long.</li>
 *         <li>... same block repeats for "oldPeers", "learners" and "oldLearners".</li>
 *     </ul></li>
 *     <li>If type is not {@link EntryType#ENTRY_TYPE_CONFIGURATION}:<ul>
 *         <li>The rest of the {@code byte[]} is the data payload.</li>
 *     </ul></li>
 * </ul>
 */
public class LogEntryV2CodecFactory implements LogEntryCodecFactory {

    // "C3PO says hello" - Using a different magic byte to distinguish V2 from V1
    public static final byte MAGIC = (byte) 0xC3;

    // Size of the magic header.
    public static final int PAYLOAD_OFFSET = 1;

    private LogEntryV2CodecFactory() {
    }

    private static final LogEntryV2CodecFactory INSTANCE = new LogEntryV2CodecFactory();

    /**
     * Returns a singleton instance of LogEntryV2CodecFactory.
     *
     * @return a singleton instance
     */
    public static LogEntryV2CodecFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public LogEntryEncoder encoder() {
        return V2Encoder.INSTANCE;
    }

    @Override
    public LogEntryDecoder decoder() {
        return V2Decoder.INSTANCE;
    }

}
