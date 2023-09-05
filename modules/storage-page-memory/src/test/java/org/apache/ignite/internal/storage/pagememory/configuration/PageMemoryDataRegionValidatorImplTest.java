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

package org.apache.ignite.internal.storage.pagememory.configuration;

import static org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
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
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * For {@link PageMemoryDataRegionValidatorImpl} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class PageMemoryDataRegionValidatorImplTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private VolatilePageMemoryStorageEngineConfiguration volatileEngineConfig;

    @InjectConfiguration
    private PersistentPageMemoryStorageEngineConfiguration persistentEngineConfig;

    @Test
    void testValidationFailForVolatilePageMemory() {
        ValidationContext<String> ctx = mockValidationContext(
                UUID.randomUUID().toString(),
                VolatilePageMemoryStorageEngineConfiguration.KEY,
                volatileEngineConfig.value(),
                mock(VolatilePageMemoryDataStorageView.class)
        );

        checkValidationFail(ctx, "Unable to find data region");
    }

    @Test
    void testValidationFailForPersistentPageMemory() {
        ValidationContext<String> ctx = mockValidationContext(
                UUID.randomUUID().toString(),
                PersistentPageMemoryStorageEngineConfiguration.KEY,
                persistentEngineConfig.value(),
                mock(PersistentPageMemoryDataStorageView.class)
        );

        checkValidationFail(ctx, "Unable to find data region");
    }

    @Test
    void testValidationFailUnknownDataStorage() {
        ValidationContext<String> ctx = mockValidationContext(
                UUID.randomUUID().toString(),
                PersistentPageMemoryStorageEngineConfiguration.KEY,
                persistentEngineConfig.value(),
                new Object()
        );

        checkValidationFail(ctx, "Unknown data storage");
    }

    @Test
    void testFindDefaultDataRegion() {
        ValidationContext<String> ctx0 = mockValidationContext(
                DEFAULT_DATA_REGION_NAME,
                VolatilePageMemoryStorageEngineConfiguration.KEY,
                volatileEngineConfig.value(),
                mock(VolatilePageMemoryDataStorageView.class)
        );

        ValidationContext<String> ctx1 = mockValidationContext(
                DEFAULT_DATA_REGION_NAME,
                PersistentPageMemoryStorageEngineConfiguration.KEY,
                persistentEngineConfig.value(),
                mock(PersistentPageMemoryDataStorageView.class)
        );

        assertThat(validate(ctx0).getAllValues(), empty());
        assertThat(validate(ctx1).getAllValues(), empty());
    }

    @Test
    void testFindOtherDataRegion() throws Exception {
        String dataRegion0 = UUID.randomUUID().toString();
        String dataRegion1 = UUID.randomUUID().toString();

        volatileEngineConfig.regions().change(c -> c.create(dataRegion0, c1 -> {})).get(1, TimeUnit.SECONDS);

        persistentEngineConfig.regions().change(c -> c.create(dataRegion1, c1 -> {})).get(1, TimeUnit.SECONDS);

        ValidationContext<String> ctx0 = mockValidationContext(
                dataRegion0,
                VolatilePageMemoryStorageEngineConfiguration.KEY,
                volatileEngineConfig.value(),
                mock(VolatilePageMemoryDataStorageView.class)
        );

        ValidationContext<String> ctx1 = mockValidationContext(
                dataRegion1,
                PersistentPageMemoryStorageEngineConfiguration.KEY,
                persistentEngineConfig.value(),
                mock(PersistentPageMemoryDataStorageView.class)
        );

        assertThat(validate(ctx0).getAllValues(), empty());
        assertThat(validate(ctx1).getAllValues(), empty());
    }

    private void checkValidationFail(ValidationContext<String> ctx, String prefixValidationMessage) {
        ArgumentCaptor<ValidationIssue> validate = validate(ctx);

        assertThat(validate.getAllValues(), hasSize(1));

        assertThat(
                validate.getValue().message(),
                is(startsWith(prefixValidationMessage))
        );
    }

    private static <T extends BasePageMemoryStorageEngineView> ValidationContext<String> mockValidationContext(
            String dataRegion,
            RootKey<?, T> rootKey,
            T engineConfigView,
            Object ownerView
    ) {
        ValidationContext<String> ctx = mock(ValidationContext.class);

        when(ctx.getNewValue()).thenReturn(dataRegion);

        when(ctx.getNewRoot(rootKey)).thenReturn(engineConfigView);

        when(ctx.getNewOwner()).thenReturn(ownerView);

        return ctx;
    }

    private static ArgumentCaptor<ValidationIssue> validate(ValidationContext<String> ctx) {
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        PageMemoryDataRegionValidatorImpl.INSTANCE.validate(null, ctx);

        return issuesCaptor;
    }
}
