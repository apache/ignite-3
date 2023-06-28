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
package org.apache.ignite.raft.jraft.entity.codec.v1;

import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;

/**
 * Log entry codec implementation. Data format description:
 * <ul>
 *     <li>Magic header, 1 byte. {@link #MAGIC}</li>
 *     <li>Log entry type, formally a var-long, effectively 1 byte. {@link EntryType}</li>
 *     <li>Log index, var-long, from 1 to 9 bytes.</li>
 *     <li>Log term, var-long, from 1 to 9 bytes.</li>
 *     <li>Checksum, 8 bytes, Big Endian.</li>
 *     <li>If type is not {@link EntryType#ENTRY_TYPE_DATA}:<ul>
 *         <li>Number of peers, val-long. Following it is the array of peers:</li>
 *         <li>Length of the peer name, 2 bytes, Big Endian.</li>
 *         <li>ASCII characters of the peer name, according to the read count.</li>
 *         <li>... same block repeats for "oldPeers", "learners" and "oldLearners".</li>
 *     </ul></li>
 *     <li>If type is not {@link EntryType#ENTRY_TYPE_DATA}:<ul>
 *         <li>The rest of the {@code byte[]} is the data payload.</li>
 *     </ul></li>
 * </ul>
 */
public class LogEntryV1CodecFactory implements LogEntryCodecFactory {

    //"Beeep boop beep beep boop beeeeeep" -BB8
    public static final byte MAGIC = (byte) 0xB8;

    // Size of the magic header.
    public static final int PAYLOAD_OFFSET = 1;

    private LogEntryV1CodecFactory() {
    }

    private static final LogEntryV1CodecFactory INSTANCE = new LogEntryV1CodecFactory();

    /**
     * Returns a singleton instance of DefaultLogEntryCodecFactory.
     *
     * @return a singleton instance
     */
    public static LogEntryV1CodecFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public LogEntryEncoder encoder() {
        return V1Encoder.INSTANCE;
    }

    @Override
    public LogEntryDecoder decoder() {
        return V1Decoder.INSTANCE;
    }

}
