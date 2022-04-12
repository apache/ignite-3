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

import static org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;

import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.store.KnownDataStorage;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Implementing a validator for {@link KnownDataStorage}.
 */
public class KnownDataStorageValidator implements Validator<KnownDataStorage, DataStorageView> {
    /** {@inheritDoc} */
    @Override
    public void validate(KnownDataStorage annotation, ValidationContext<DataStorageView> ctx) {
        DataStorageView newValue = ctx.getNewValue();

        if (newValue.name().equals(UNKNOWN_DATA_STORAGE)) {
            ctx.addIssue(new ValidationIssue(String.format(
                    "Data storage cannot be '%s': %s",
                    UNKNOWN_DATA_STORAGE,
                    ctx.currentKey()
            )));
        }
    }
}
