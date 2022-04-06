/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.configuration;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockAddIssue;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.store.KnownDataStorage;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageChange;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageConfigurationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * For {@link KnownDataStorageValidator} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class KnownDataStorageValidatorTest {
    @InjectConfiguration(
            polymorphicExtensions = {UnknownDataStorageConfigurationSchema.class, TestDataStorageConfigurationSchema.class}
    )
    private DataStorageConfiguration dataStorageConfig;

    @Test
    void testFailValidation() {
        KnownDataStorage annotation = mock(KnownDataStorage.class);

        DataStorageView value = dataStorageConfig.value();

        validate(annotation, mockValidationContext(value, value), false);
    }

    @Test
    void testSuccessValidation() throws Exception {
        KnownDataStorage annotation = mock(KnownDataStorage.class);

        dataStorageConfig.change(c -> c.convert(TestDataStorageChange.class)).get(1, SECONDS);

        DataStorageView value = dataStorageConfig.value();

        validate(annotation, mockValidationContext(value, value), true);
    }

    private static void validate(
            KnownDataStorage annotation,
            ValidationContext<DataStorageView> ctx,
            boolean expEmpty
    ) {
        ArgumentCaptor<ValidationIssue> argumentCaptor = mockAddIssue(ctx);

        KnownDataStorageValidator.INSTANCE.validate(annotation, ctx);

        if (expEmpty) {
            assertThat(argumentCaptor.getAllValues(), empty());
        } else {
            assertThat(argumentCaptor.getAllValues(), hasSize(1));

            assertThat(argumentCaptor.getValue().message(), startsWith("Data storage cannot be 'unknown'"));
        }
    }
}
