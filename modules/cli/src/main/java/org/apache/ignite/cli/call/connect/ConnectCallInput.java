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

package org.apache.ignite.cli.call.connect;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for connect call to Ignite 3 node.
 */
public class ConnectCallInput implements CallInput {
    private final String nodeUrl;

    private final String commandName;

    ConnectCallInput(String nodeUrl, String commandName) {
        this.nodeUrl = nodeUrl;
        this.commandName = commandName;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public String getCommandName() {
        return commandName;
    }

    public static ConnectCallInputBuilder builder() {
        return new ConnectCallInputBuilder();
    }

    /**
     * Builder for {@link ConnectCall}.
     */
    public static class ConnectCallInputBuilder {
        private String nodeUrl;

        private String commandName;

        public ConnectCallInputBuilder nodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
            return this;
        }

        public ConnectCallInputBuilder commandName(String commandName) {
            this.commandName = commandName;
            return this;
        }

        public ConnectCallInput build() {
            return new ConnectCallInput(nodeUrl, commandName);
        }
    }
}
