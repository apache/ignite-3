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

package org.apache.ignite.internal.schemasync;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class ItSchemaSyncConfigTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void delayDurationIsImmutable() {
        SchemaSynchronizationConfiguration config = unwrapIgniteImpl(CLUSTER.aliveNode()).clusterConfiguration()
                .getConfiguration(SchemaSynchronizationConfiguration.KEY);

        ConfigurationChangeException ex = assertWillThrowFast(
                config.delayDuration().update(config.delayDuration().value() + 100),
                ConfigurationChangeException.class
        );

        assertThat(ex.getCause(), is(instanceOf(ConfigurationValidationException.class)));
        assertThat(
                ex.getCause().getMessage(),
                containsString("Validation did not pass for keys: [schemaSync.delayDuration, 'schemaSync.delayDuration' "
                        + "configuration value is immutable and cannot be updated")
        );
    }

    @Test
    void maxClockSkewIsImmutable() {
        SchemaSynchronizationConfiguration config = unwrapIgniteImpl(CLUSTER.aliveNode()).clusterConfiguration()
                .getConfiguration(SchemaSynchronizationConfiguration.KEY);

        ConfigurationChangeException ex = assertWillThrowFast(
                config.maxClockSkew().update(config.maxClockSkew().value() + 100),
                ConfigurationChangeException.class
        );

        assertThat(ex.getCause(), is(instanceOf(ConfigurationValidationException.class)));
        assertThat(
                ex.getCause().getMessage(),
                containsString("Validation did not pass for keys: [schemaSync.maxClockSkew, 'schemaSync.maxClockSkew' "
                        + "configuration value is immutable and cannot be updated")
        );
    }
}
