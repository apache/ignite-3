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

package org.apache.ignite.internal.cli.decorators;

import org.apache.ignite.internal.cli.call.cliconfig.profile.ProfileList;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStatus;
import org.apache.ignite.internal.cli.call.configuration.JsonString;
import org.apache.ignite.internal.cli.call.node.status.NodeStatus;
import org.apache.ignite.internal.cli.config.Profile;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.DecoratorRegistry;
import org.apache.ignite.internal.cli.sql.SqlQueryResult;
import org.apache.ignite.internal.cli.sql.table.Table;

/**
 * Default set of {@link Decorator}.
 */
public class DefaultDecoratorRegistry extends DecoratorRegistry {
    /**
     * Constructor.
     */
    public DefaultDecoratorRegistry() {
        add(JsonString.class, new JsonDecorator(false));
        add(Profile.class, new ProfileDecorator());
        add(ProfileList.class, new ProfileListDecorator());
        add(Table.class, new TableDecorator(false));
        add(SqlQueryResult.class, new SqlQueryResultDecorator(false));
        add(ClusterStatus.class, new ClusterStatusDecorator());
        add(NodeStatus.class, new NodeStatusDecorator());
    }
}
