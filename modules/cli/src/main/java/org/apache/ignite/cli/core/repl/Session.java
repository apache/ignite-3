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

package org.apache.ignite.cli.core.repl;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.config.ConfigConstants;
import org.apache.ignite.cli.config.StateConfig;

/**
 * Connection session that in fact is holder for state: connected or disconnected.
 * Also has a nodeUrl if the state is connected.
 */
@Singleton
public class Session {

    private boolean connectedToNode;

    private String nodeUrl;

    private String nodeName;

    private String jdbcUrl;

    private final Config config = StateConfig.getStateConfig();

    public boolean isConnectedToNode() {
        return connectedToNode;
    }

    public void setConnectedToNode(boolean connectedToNode) {
        this.connectedToNode = connectedToNode;
    }

    public String nodeUrl() {
        return nodeUrl;
    }

    public String nodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setNodeUrl(String nodeUrl) {
        this.nodeUrl = nodeUrl;
        if (nodeUrl != null) {
            config.setProperty(ConfigConstants.LAST_CONNECTED_URL, nodeUrl);
        }
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
}
