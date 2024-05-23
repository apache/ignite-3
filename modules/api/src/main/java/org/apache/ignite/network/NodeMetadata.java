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

    private final int httpPort;

    private final int httpsPort;

    /** Constructor. */
    public NodeMetadata(String restHost, int httpPort, int httpsPort) {
        this.restHost = restHost;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
    }

    public String restHost() {
        return restHost;
    }

    public int httpPort() {
        return httpPort;
    }

    public int httpsPort() {
        return httpsPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeMetadata that = (NodeMetadata) o;

        if (httpPort != that.httpPort) {
            return false;
        }
        if (httpsPort != that.httpsPort) {
            return false;
        }
        return restHost != null ? restHost.equals(that.restHost) : that.restHost == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(restHost, httpPort, httpsPort);
    }

    @Override
    public String toString() {
        return "NodeMetadata{"
                + "restHost='" + restHost + '\''
                + ", httpPort=" + httpPort
                + ", httpsPort=" + httpsPort
                + '}';
    }
}
