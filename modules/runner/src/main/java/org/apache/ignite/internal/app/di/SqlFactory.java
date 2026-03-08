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

package org.apache.ignite.internal.app.di;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.sql.configuration.distributed.SqlClusterExtensionConfiguration;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlNodeExtensionConfiguration;

/**
 * Micronaut factory for SQL engine components.
 */
@Factory
public class SqlFactory {
    /** Creates the SQL distributed configuration from the cluster config registry. */
    @Singleton
    public SqlDistributedConfiguration sqlDistributedConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(SqlClusterExtensionConfiguration.KEY).sql();
    }

    /** Creates the SQL local configuration from the node config registry. */
    @Singleton
    public SqlLocalConfiguration sqlLocalConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(SqlNodeExtensionConfiguration.KEY).sql();
    }

}
