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

package org.apache.ignite.internal.cli.call.configuration;

import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Input for {@link NodeConfigShowCall}.
 */
public class NodeConfigShowCallInput implements CallInput {
    /**
     * Selector for configuration tree.
     */
    private final String selector;

    /**
     * Node url.
     */
    private final String nodeUrl;

    private NodeConfigShowCallInput(String selector, String nodeUrl) {
        this.nodeUrl = nodeUrl;
        this.selector = selector;
    }

    /**
     * Builder for {@link NodeConfigShowCallInput}.
     */
    public static ShowConfigurationCallInputBuilder builder() {
        return new ShowConfigurationCallInputBuilder();
    }

    /**
     * Get selector.
     *
     * @return Selector for configuration tree.
     */
    public String getSelector() {
        return selector;
    }

    /**
     * Get node URL.
     *
     * @return Cluster URL.
     */
    public String getNodeUrl() {
        return nodeUrl;
    }

    /**
     * Builder for {@link NodeConfigShowCallInput}.
     */
    public static class ShowConfigurationCallInputBuilder {

        private String selector;

        private String nodeUrl;

        public ShowConfigurationCallInputBuilder selector(String selector) {
            this.selector = selector;
            return this;
        }

        public ShowConfigurationCallInputBuilder nodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
            return this;
        }

        public NodeConfigShowCallInput build() {
            return new NodeConfigShowCallInput(selector, nodeUrl);
        }
    }
}
