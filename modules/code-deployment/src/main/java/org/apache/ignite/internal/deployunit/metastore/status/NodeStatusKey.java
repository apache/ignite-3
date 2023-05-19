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

import org.apache.ignite.internal.deployunit.version.Version;
import org.apache.ignite.lang.ByteArray;

/**
 *  Deployment unit node status store key.
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
    public NodeStatusKey(String id, Version version, String nodeId) {
        this.id = id;
        this.version = version;
        this.nodeId = nodeId;
    }


    public ByteArray toKey() {
        return UnitKey.toKey(NODES_PREFIX, id, version == null ? null : version.render(), nodeId);
    }

    /**
     * Deserializer method.
     *
     * @param key Serialized node status key.
     * @return Deserialized node status key.
     */
    public static NodeStatusKey fromKey(byte[] key) {
        String[] parse = UnitKey.fromKey(NODES_PREFIX, key);
        int length = parse.length;
        String id = length > 0 ? parse[0] : null;
        Version version = length > 1 ? Version.parseVersion(parse[1]) : null;
        String nodeId = length > 2 ? parse[2] : null;

        return builder().withId(id).withVersion(version).withNodeId(nodeId).build();
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

    /**
     * Builder for {@link NodeStatusKey}.
     */
    public static class NodeStatusKeyBuilder {
        private String id;

        private Version version;

        private String nodeId;

        public NodeStatusKeyBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public NodeStatusKeyBuilder withVersion(Version version) {
            this.version = version;
            return this;
        }

        public NodeStatusKeyBuilder withNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public NodeStatusKey build() {
            return new NodeStatusKey(id, version, nodeId);
        }
    }
}
