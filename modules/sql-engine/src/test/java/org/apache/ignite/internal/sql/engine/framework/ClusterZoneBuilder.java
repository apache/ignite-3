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

package org.apache.ignite.internal.sql.engine.framework;

import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.ClusterBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.NestedBuilder;

/**
 * A builder interface for creating a test zone as a nested object within a test cluster.
 *
 * <p>
 * This builder allows the configuration and construction of test zones, which are integral parts of the {@link TestCluster}.
 * Test zones can be customized and linked to the enclosing cluster through this builder.
 * </p>
 *
 * <p>
 * The {@link ClusterZoneBuilder} extends {@link ZoneBuilderBase}, inheriting common methods for zone configuration,
 * and implements {@link NestedBuilder} to ensure fluent integration into the cluster creation process.
 * </p>
 *
 * @see TestCluster
 * @see ZoneBuilderBase
 * @see NestedBuilder
 */
public interface ClusterZoneBuilder extends ZoneBuilderBase<ClusterZoneBuilder>, NestedBuilder<ClusterBuilder> {
    CatalogCommand build();
}