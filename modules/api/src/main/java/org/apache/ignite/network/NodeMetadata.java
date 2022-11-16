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

import java.io.Serializable;
import java.util.Objects;

/**
 * Contains metadata of the cluster node.
 */
public class NodeMetadata implements Serializable {
    private static final long serialVersionUID = 3216463261002854096L;

    private final String restHost;

    private final int restPort;

    public NodeMetadata(String restHost, int restPort) {
        this.restHost = restHost;
        this.restPort = restPort;
    }

    public String restHost() {
        return restHost;
    }

    public int restPort() {
        return restPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeMetadata)) {
            return false;
        }
        NodeMetadata that = (NodeMetadata) o;
        return restPort == that.restPort && Objects.equals(restHost, that.restHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(restHost, restPort);
    }
}
