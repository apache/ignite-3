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

package org.apache.ignite.internal.storage.configuration;

import static org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.schemas.store.ExistDataStorage;
import org.apache.ignite.internal.storage.engine.StorageEngineFactory;
import org.junit.jupiter.api.Test;

/**
 * For {@link ExistDataStorageValidator} testing.
 */
public class ExistDataStorageValidatorTest {
    @Test
    void testFailValidation() {
        ExistDataStorage annotation = mock(ExistDataStorage.class);

        String dataStorage1 = UUID.randomUUID().toString();
        String dataStorage2 = UUID.randomUUID().toString();

        ExistDataStorageValidator validator = new ExistDataStorageValidator(List.of(
                createMockedStorageEngineFactory(dataStorage1),
                createMockedStorageEngineFactory(dataStorage2)
        ));

        String value = UUID.randomUUID().toString();

        validate(validator, annotation, mockValidationContext(value, value), "Non-existent data storage");
    }

    @Test
    void testSuccessValidation() {
        ExistDataStorage annotation = mock(ExistDataStorage.class);

        String dataStorage1 = UUID.randomUUID().toString();
        String dataStorage2 = UUID.randomUUID().toString();

        ExistDataStorageValidator validator = new ExistDataStorageValidator(List.of(
                createMockedStorageEngineFactory(dataStorage1),
                createMockedStorageEngineFactory(dataStorage2)
        ));

        validate(validator, annotation, mockValidationContext(UNKNOWN_DATA_STORAGE, UNKNOWN_DATA_STORAGE), null);
        validate(validator, annotation, mockValidationContext(dataStorage1, dataStorage1), null);
        validate(validator, annotation, mockValidationContext(dataStorage2, dataStorage2), null);
    }

    private StorageEngineFactory createMockedStorageEngineFactory(String name) {
        StorageEngineFactory mock = mock(StorageEngineFactory.class);

        when(mock.name()).thenReturn(name);

        return mock;
    }
}
