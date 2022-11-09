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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.metadata.JdkMetadataCodec;
import java.nio.ByteBuffer;
import org.apache.ignite.network.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/** Deserializer for {@link org.apache.ignite.network.NodeMetadata}. */
public class NodeMetadataDeserializer {

    private final JdkMetadataCodec metadataCodec = new JdkMetadataCodec();

    /**
     * Deserializes {@link ByteBuffer} to {@link NodeMetadata}.
     *
     * @param byteBuffer {@link ByteBuffer} to deserialize.
     * @return {@link NodeMetadata} or null if something goes wrong.
     */
    @Nullable
    public NodeMetadata deserialize(@Nullable ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        try {
            return (NodeMetadata) metadataCodec.deserialize(byteBuffer);
        } catch (Exception e) {
            return null;
        }
    }
}
