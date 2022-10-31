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

import io.scalecube.cluster.metadata.MetadataCodec;
import java.nio.ByteBuffer;
import org.apache.ignite.network.NodeMetadata;

public class NodeMetadataCodec implements MetadataCodec {
    @Override
    public NodeMetadata deserialize(ByteBuffer byteBuffer) {
        return NodeMetadata.fromByteBuffer(byteBuffer);
    }

    @Override
    public ByteBuffer serialize(Object o) {
        if(o instanceof NodeMetadata) {
            return ((NodeMetadata) o).toByteBuffer();
        } else {
            return ByteBuffer.wrap(new byte[0]);
        }
    }
}
