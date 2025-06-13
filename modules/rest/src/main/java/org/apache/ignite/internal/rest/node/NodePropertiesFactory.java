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
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.rest.RestFactory;

/**
 * Factory that defines {@link NodeProperties} bean.
 */
@Factory
public class NodePropertiesFactory implements RestFactory {
    private NodeProperties nodeProperties;

    public NodePropertiesFactory(NodeProperties nodeProperties) {
        this.nodeProperties = nodeProperties;
    }

    @Bean
    @Singleton
    public NodeProperties nodeProperties() {
        return nodeProperties;
    }

    @Override
    public void cleanResources() {
        nodeProperties = null;
    }
}
