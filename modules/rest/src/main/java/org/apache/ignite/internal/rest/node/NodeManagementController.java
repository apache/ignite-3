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

package org.apache.ignite.internal.rest.node;

import io.micronaut.http.annotation.Controller;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.rest.RestFactory;
import org.apache.ignite.internal.rest.api.node.NodeInfo;
import org.apache.ignite.internal.rest.api.node.NodeManagementApi;
import org.apache.ignite.internal.rest.api.node.NodeState;

/**
 * REST endpoint allows to read node state.
 */
@Controller("/management/v1/node")
public class NodeManagementController implements NodeManagementApi, RestFactory {
    private StateProvider stateProvider;

    private NameProvider nameProvider;

    private JdbcPortProvider jdbcPortProvider;

    /**
     * Constructs node management controller.
     */
    public NodeManagementController(NameProvider nameProvider, StateProvider stateProvider, JdbcPortProvider jdbcPortProvider) {
        this.nameProvider = nameProvider;
        this.stateProvider = stateProvider;
        this.jdbcPortProvider = jdbcPortProvider;
    }

    @Override
    public NodeState state() {
        return new NodeState(nameProvider.getName(), stateProvider.getState());
    }

    @Override
    public NodeInfo info() {
        return new NodeInfo(nameProvider.getName(), jdbcPortProvider.jdbcPort());
    }

    @Override
    public String version() {
        return IgniteProductVersion.CURRENT_VERSION.toString();
    }

    @Override
    public void cleanResources() {
        nameProvider = null;
        stateProvider = null;
        jdbcPortProvider = null;
    }
}
