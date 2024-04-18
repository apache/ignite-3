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

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.RestFactory;

/**
 * Node management REST factory defines beans needed for {@link NodeManagementController}.
 */
@Factory
public class NodeManagementRestFactory implements RestFactory {
    private StateProvider stateProvider;

    private NameProvider nameProvider;

    private JdbcPortProvider jdbcPortProvider;

    /**
     * Constructs node management rest factory.
     */
    public NodeManagementRestFactory(StateProvider stateProvider, NameProvider nameProvider, JdbcPortProvider jdbcPortProvider) {
        this.stateProvider = stateProvider;
        this.nameProvider = nameProvider;
        this.jdbcPortProvider = jdbcPortProvider;
    }

    @Singleton
    @Bean
    public StateProvider stateProvider() {
        return stateProvider;
    }

    @Singleton
    @Bean
    public NameProvider nameProvider() {
        return nameProvider;
    }

    @Singleton
    @Bean
    public JdbcPortProvider jdbcPortProvider() {
        return jdbcPortProvider;
    }

    @Override
    public void cleanResources() {
        stateProvider = null;
        nameProvider = null;
        jdbcPortProvider = null;
    }
}
