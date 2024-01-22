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

package org.apache.ignite.internal.rest.compute;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.compute.ComputeComponent;
import org.apache.ignite.internal.rest.RestFactory;

/**
 * Factory that creates beans that are needed for {@link ComputeController}.
 */
@Factory
public class ComputeRestFactory implements RestFactory {
    private ComputeComponent computeComponent;

    public ComputeRestFactory(ComputeComponent computeComponent) {
        this.computeComponent = computeComponent;
    }

    @Bean
    @Singleton
    public ComputeComponent computeComponent() {
        return computeComponent;
    }

    @Override
    public void cleanResources() {
        computeComponent = null;
    }
}
