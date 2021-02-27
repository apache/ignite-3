/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.rpc;

/**
 * An node with an immutable id.
 * TODO FIXME asch must be elsewhere.
 */
public class NodeImpl implements Node {
    private final String id;

    public NodeImpl(String id) {
        super();
        this.id = id;
    }

    @Override public String id() {
        return id;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeImpl node = (NodeImpl) o;

        if (!id.equals(node.id)) return false;

        return true;
    }

    @Override public int hashCode() {
        return id.hashCode();
    }
}
