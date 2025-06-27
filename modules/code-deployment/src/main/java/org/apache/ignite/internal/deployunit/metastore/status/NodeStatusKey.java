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

package org.apache.ignite.internal.deployunit.metastore.status;

import static org.apache.ignite.internal.deployunit.metastore.status.UnitKey.DEPLOY_UNIT_PREFIX;

import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.lang.ByteArray;

/**
 * Deployment unit node status store key.
 */
public class NodeStatusKey {
    private static final String NODES_PREFIX = DEPLOY_UNIT_PREFIX + "nodes.";

    private final String id;

    private final Version version;

    private final String nodeId;

    /**
     * Constructor.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param nodeId Cluster node consistent identifier.
     */
    private NodeStatusKey(String id, Version version, String nodeId) {
        this.id = id;
        this.version = version;
        this.nodeId = nodeId;
    }

    /**
     * Serialize key instance to {@link ByteArray}.
     *
     * @return {@link ByteArray} instance with serialized content.
     */
    public ByteArray toByteArray() {
        return UnitKey.toByteArray(NODES_PREFIX, id, version == null ? null : version.render(), nodeId);
    }

    /**
     * Deserializer method.
     *
     * @param key Serialized node status key.
     * @return Deserialized node status key.
     */
    public static NodeStatusKey fromBytes(byte[] key) {
        String[] parse = UnitKey.fromBytes(NODES_PREFIX, key);
        int length = parse.length;
        String id = length > 0 ? parse[0] : null;
        Version version = length > 1 ? Version.parseVersion(parse[1]) : null;
        String nodeId = length > 2 ? parse[2] : null;

        return builder().id(id).version(version).nodeId(nodeId).build();
    }

    /**
     * Returns builder {@link NodeStatusKeyBuilder}.
     *
     * @return builder {@link NodeStatusKeyBuilder}.
     */
    public static NodeStatusKeyBuilder builder() {
        return new NodeStatusKeyBuilder();
    }

    public String id() {
        return id;
    }

    public Version version() {
        return version;
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeStatusKey that = (NodeStatusKey) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (version != null ? !version.equals(that.version) : that.version != null) {
            return false;
        }
        return nodeId != null ? nodeId.equals(that.nodeId) : that.nodeId == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
        return result;
    }

    /**
     * Builder for {@link NodeStatusKey}.
     */
    public static class NodeStatusKeyBuilder {
        private String id;

        private Version version;

        private String nodeId;

        public NodeStatusKeyBuilder id(String id) {
            this.id = id;
            return this;
        }

        public NodeStatusKeyBuilder version(Version version) {
            this.version = version;
            return this;
        }

        public NodeStatusKeyBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public NodeStatusKey build() {
            return new NodeStatusKey(id, version, nodeId);
        }
    }
}
