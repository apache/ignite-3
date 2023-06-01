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

package org.apache.ignite.internal.cli.call.cluster.unit;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.repl.registry.UnitsRegistry;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;

/** Factory for {@link UndeployUnitReplCall} in REPL mode. */
@Factory
public class UndeployUnitReplCallFactory {

    private final ApiClientFactory factory;

    private final UnitsRegistry registry;

    public UndeployUnitReplCallFactory(ApiClientFactory factory, UnitsRegistry registry) {
        this.factory = factory;
        this.registry = registry;
    }

    @Bean
    @Singleton
    public UndeployUnitReplCall create() {
        return new UndeployUnitReplCall(factory, registry);
    }
}
