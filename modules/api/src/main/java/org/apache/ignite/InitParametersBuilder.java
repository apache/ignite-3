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

package org.apache.ignite;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.rest.AuthenticationConfig;

/** Builder of {@link org.apache.ignite.InitParameters}. */
public class InitParametersBuilder {
    private String nodeName;
    private Collection<String> metaStorageNodeNames = List.of();
    private Collection<String> cmgNodeNames = List.of();
    private String clusterName;
    private AuthenticationConfig authenticationConfig = AuthenticationConfig.disabled();

    public InitParametersBuilder setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    public InitParametersBuilder setMetaStorageNodeNames(Collection<String> metaStorageNodeNames) {
        this.metaStorageNodeNames = metaStorageNodeNames;
        return this;
    }

    public InitParametersBuilder setCmgNodeNames(Collection<String> cmgNodeNames) {
        this.cmgNodeNames = cmgNodeNames;
        return this;
    }

    public InitParametersBuilder setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public InitParametersBuilder setRestAuthenticationConfig(AuthenticationConfig authenticationConfig) {
        this.authenticationConfig = authenticationConfig;
        return this;
    }

    public InitParameters build() {
        cmgNodeNames = cmgNodeNames.isEmpty() ? metaStorageNodeNames : cmgNodeNames;

        Objects.requireNonNull(nodeName);
        Objects.requireNonNull(metaStorageNodeNames);
        Objects.requireNonNull(cmgNodeNames);
        Objects.requireNonNull(clusterName);
        Objects.requireNonNull(authenticationConfig);

        return new InitParameters(nodeName, metaStorageNodeNames, cmgNodeNames, clusterName, authenticationConfig);
    }
}
