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

import org.apache.ignite.raft.jraft.entity.codec.v1.V1Decoder;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Encoder;

/**
 * Default log entry codec factory
 */
public class DefaultLogEntryCodecFactory implements LogEntryCodecFactory {

    private DefaultLogEntryCodecFactory() {
    }

    private static DefaultLogEntryCodecFactory INSTANCE = new DefaultLogEntryCodecFactory();

    /**
     * Returns a singleton instance of DefaultLogEntryCodecFactory.
     *
     * @return a singleton instance
     */
    public static DefaultLogEntryCodecFactory getInstance() {
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
