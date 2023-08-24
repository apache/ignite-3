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

package org.apache.ignite.internal.storage.rocksdb.configuration;

import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * For {@link RocksDbDataRegionValidatorImpl} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class RocksDbDataRegionValidatorImplTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private RocksDbStorageEngineConfiguration engineConfig;

    @Test
    void testValidationFail() {
        ValidationContext<String> ctx = mockValidationContext(UUID.randomUUID().toString(), engineConfig.value());

        ArgumentCaptor<ValidationIssue> validate = validate(ctx);

        assertThat(validate.getAllValues(), hasSize(1));

        assertThat(
                validate.getValue().message(),
                is(startsWith("Unable to find data region"))
        );
    }

    @Test
    void testFindDefaultDataRegion() {
        ValidationContext<String> ctx = mockValidationContext(DEFAULT_DATA_REGION_NAME, engineConfig.value());

        ArgumentCaptor<ValidationIssue> validate = validate(ctx);

        assertThat(validate.getAllValues(), empty());
    }

    @Test
    void testFindOtherDataRegion() throws Exception {
        String dataRegion = UUID.randomUUID().toString();

        engineConfig.regions().change(c -> c.create(dataRegion, c1 -> {
        })).get(1, TimeUnit.SECONDS);

        ValidationContext<String> ctx = mockValidationContext(dataRegion, engineConfig.value());

        ArgumentCaptor<ValidationIssue> validate = validate(ctx);

        assertThat(validate.getAllValues(), empty());
    }

    private static ValidationContext<String> mockValidationContext(String dataRegion, RocksDbStorageEngineView engineConfigView) {
        ValidationContext<String> ctx = mock(ValidationContext.class);

        when(ctx.getNewValue()).thenReturn(dataRegion);

        when(ctx.getNewRoot(RocksDbStorageEngineConfiguration.KEY)).thenReturn(engineConfigView);

        return ctx;
    }

    private static ArgumentCaptor<ValidationIssue> validate(ValidationContext<String> ctx) {
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        RocksDbDataRegionValidatorImpl.INSTANCE.validate(null, ctx);

        return issuesCaptor;
    }
}
