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

package org.apache.ignite.network;

import java.nio.ByteBuffer;

/**
 * Arasdasda.
 */
public class NodeMetadata {
    private static final int VERSION = 1;
    private final int restPort;

    public NodeMetadata(int restPort) {
        this.restPort = restPort;
    }

    public int restPort() {
        return restPort;
    }

    /**
     *
     */
    public static NodeMetadata fromByteBuffer(ByteBuffer metadata) {
        int version = metadata.getInt(0);
        if (version == VERSION) {
            byte port = metadata.get(1);
            return new NodeMetadata(port);
        } else {
            return null;
        }
    }

    /**
     *
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(VERSION);
        buffer.putInt(restPort);
        return buffer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeMetadata that = (NodeMetadata) o;

        return restPort == that.restPort;
    }

    @Override
    public int hashCode() {
        return restPort;
    }
}
