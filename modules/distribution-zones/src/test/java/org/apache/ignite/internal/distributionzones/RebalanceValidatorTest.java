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

package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.IsReplicasChangeAvailable;
import org.apache.ignite.internal.distributionzones.configuration.RebalanceValidator;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link RebalanceValidator}.
 */
// TODO: Remove under the IGNITE-19424
@ExtendWith(ConfigurationExtension.class)
public class RebalanceValidatorTest {

    @Test
    void testValidWithTheOnlyOneTablePerZone(
            @InjectConfiguration("mock.tables {foo1 {zoneId = 1}, foo2 {zoneId = 2}}")
            TablesConfiguration tablesConfig,
            @InjectConfiguration("mock {zoneId = 1, replicas = 1}")
            DistributionZoneConfiguration distributionZoneConfiguration

    ) {
        IsReplicasChangeAvailable annotation = mock(IsReplicasChangeAvailable.class);

        ValidationContext<Integer> validationContext = mockValidationContext(
                distributionZoneConfiguration.replicas().value(),
                distributionZoneConfiguration.replicas().value() + 1);

        DistributionZoneView distributionZoneView = distributionZoneConfiguration.value();
        when(validationContext.getNewOwner()).thenReturn(distributionZoneView);

        TablesView tablesView = tablesConfig.value();
        when(validationContext.getNewRoot(TablesConfiguration.KEY)).thenReturn(tablesView);

        TestValidationUtil.validate(new RebalanceValidator(), annotation, validationContext);
    }

    @Test
    void testValidWithTheSameOldAndNewValue(
            @InjectConfiguration("mock.tables {foo1 {zoneId = 1}, foo2 {zoneId = 1}}")
            TablesConfiguration tablesConfig,
            @InjectConfiguration("mock {zoneId = 1, replicas = 1}")
            DistributionZoneConfiguration distributionZoneConfiguration
    ) {
        IsReplicasChangeAvailable annotation = mock(IsReplicasChangeAvailable.class);

        ValidationContext<Integer> validationContext = mockValidationContext(
                distributionZoneConfiguration.replicas().value(),
                distributionZoneConfiguration.replicas().value());

        DistributionZoneView distributionZoneView = distributionZoneConfiguration.value();
        when(validationContext.getNewOwner()).thenReturn(distributionZoneView);

        TablesView tablesView = tablesConfig.value();
        when(validationContext.getNewRoot(TablesConfiguration.KEY)).thenReturn(tablesView);

        TestValidationUtil.validate(new RebalanceValidator(), annotation, validationContext);
    }

    @Test
    void testInvalid(
            @InjectConfiguration("mock.tables {foo1 {zoneId = 1}, foo2 {zoneId = 1}}")
            TablesConfiguration tablesConfig,
            @InjectConfiguration("mock {zoneId = 1, replicas = 1}")
            DistributionZoneConfiguration distributionZoneConfiguration
    ) {
        IsReplicasChangeAvailable annotation = mock(IsReplicasChangeAvailable.class);

        ValidationContext<Integer> validationContext = mockValidationContext(
                distributionZoneConfiguration.replicas().value(),
                distributionZoneConfiguration.replicas().value() + 1);

        DistributionZoneView distributionZoneView = distributionZoneConfiguration.value();
        when(validationContext.getNewOwner()).thenReturn(distributionZoneView);

        TablesView tablesView = tablesConfig.value();
        when(validationContext.getNewRoot(TablesConfiguration.KEY)).thenReturn(tablesView);

        TestValidationUtil.validate(new RebalanceValidator(), annotation, validationContext,
                "Change the number of replicas fot the zone with the > 1 tables is not supported yet");
    }
}
