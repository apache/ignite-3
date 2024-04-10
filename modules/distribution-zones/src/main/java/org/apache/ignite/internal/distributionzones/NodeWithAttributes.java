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

package org.apache.ignite.internal.distributionzones;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.jetbrains.annotations.Nullable;

/**
 * Structure that represents node with the attributes and which we store in Meta Storage when we store logical topology.
 * Light-weighted version of the {@link LogicalNode}.
 */
public class NodeWithAttributes implements Serializable {
    private static final long serialVersionUID = -7778967985161743937L;

    private final Node node;

    private final Map<String, String> userAttributes;

    private final List<String> storageProfiles;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param nodeId Node consistent identifier.
     * @param userAttributes Key value map of user's node's attributes.
     */
    public NodeWithAttributes(String nodeName, String nodeId, @Nullable Map<String, String> userAttributes) {
        this(nodeName, nodeId, userAttributes, List.of());
    }

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param nodeId Node consistent identifier.
     * @param userAttributes Key value map of user's node's attributes.
     * @param storageProfiles List of supported storage profiles on the node.
     */
    public NodeWithAttributes(
            String nodeName,
            String nodeId,
            @Nullable Map<String, String> userAttributes,
            @Nullable List<String> storageProfiles) {
        this.node = new Node(nodeName, nodeId);
        this.userAttributes = userAttributes == null ? Map.of() : userAttributes;
        this.storageProfiles = storageProfiles == null ? List.of() : storageProfiles;
    }

    @Override
    public int hashCode() {
        return node.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        NodeWithAttributes that = (NodeWithAttributes) obj;

        return node.equals(that.node)
                && this.userAttributes.equals(that.userAttributes)
                && this.storageProfiles.equals(that.storageProfiles);
    }

    public String nodeName() {
        return node.nodeName();
    }

    public String nodeId() {
        return node.nodeId();
    }

    public Node node() {
        return node;
    }

    public Map<String, String> userAttributes() {
        return userAttributes;
    }

    public List<String> storageProfiles() {
        return storageProfiles;
    }

    @Override
    public String toString() {
        return node.toString();
    }
}
