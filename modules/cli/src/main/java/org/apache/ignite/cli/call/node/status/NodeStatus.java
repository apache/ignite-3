/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.call.node.status;

/**
 * Represents the status of the node.
 */
public class NodeStatus {
    private final String name;

    private final State state;

    private NodeStatus(String name, State state) {
        this.name = name;
        this.state = state;
    }

    public String name() {
        return name;
    }

    public State state() {
        return state;
    }

    /** Builder for {@link NodeStatus}. */
    public static NodeStatusBuilder builder() {
        return new NodeStatusBuilder();
    }

    /** Builder for {@link NodeStatus}. */
    public static class NodeStatusBuilder {
        private String name;

        private State state;

        public NodeStatusBuilder name(String name) {
            this.name = name;
            return this;
        }

        public NodeStatusBuilder state(State state) {
            this.state = state;
            return this;
        }

        public NodeStatusBuilder state(String stateName) {
            this.state = State.valueOf(stateName.toUpperCase());
            return this;
        }

        public NodeStatus build() {
            return  new NodeStatus(name, state);
        }
    }
}
