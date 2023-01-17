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

package org.apache.ignite.internal.schema.configuration;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageChange;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageView;
import org.apache.ignite.internal.schema.configuration.storage.KnownDataStorageValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link KnownDataStorageValidator} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class KnownDataStorageValidatorTest {
    @InjectConfiguration(
            polymorphicExtensions = {
                TestDataStorageConfigurationSchema.class
            }
    )
    private DataStorageConfiguration dataStorageConfig;

    @Test
    void testFailValidation() {
        var annotation = mock(ConfigValue.class);

        DataStorageView value = dataStorageConfig.value();

        validate(new KnownDataStorageValidator(), annotation, mockValidationContext(value, value), "Data storage cannot be 'unknown'");
    }

    @Test
    void testSuccessValidation() throws Exception {
        var annotation = mock(ConfigValue.class);

        dataStorageConfig.change(c -> c.convert(TestDataStorageChange.class)).get(1, SECONDS);

        DataStorageView value = dataStorageConfig.value();

        validate(new KnownDataStorageValidator(), annotation, mockValidationContext(value, value), null);
    }
}
