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

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;

import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.configuration.schemas.store.ExistingDataStorage;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.storage.DataStorageModule;

/**
 * Implementing a validator for {@link ExistingDataStorage}.
 */
public class ExistingDataStorageValidator implements Validator<ExistingDataStorage, String> {
    private final Set<String> dataStorages;

    /**
     * Constructor.
     *
     * @param modules Storage engine factories.
     */
    public ExistingDataStorageValidator(Iterable<DataStorageModule> modules) {
        dataStorages = Stream.concat(
                Stream.of(UNKNOWN_DATA_STORAGE),
                StreamSupport.stream(modules.spliterator(), false).map(DataStorageModule::name)
        ).collect(toUnmodifiableSet());
    }

    /** {@inheritDoc} */
    @Override
    public void validate(ExistingDataStorage annotation, ValidationContext<String> ctx) {
        String newValue = ctx.getNewValue();

        if (!dataStorages.contains(newValue)) {
            ctx.addIssue(new ValidationIssue(String.format(
                    "Non-existent data storage [dataStorage=%s, existDataStorages=%s, key=%s]",
                    newValue,
                    dataStorages,
                    ctx.currentKey()
            )));
        }
    }
}
