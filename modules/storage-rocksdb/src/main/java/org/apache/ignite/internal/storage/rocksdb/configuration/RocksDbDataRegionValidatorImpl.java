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

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.storage.configurations.StoragesConfiguration;
import org.apache.ignite.internal.storage.configurations.StoragesView;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageProfileConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageProfileConfigurationSchema;

/**
 * Implementing a validator for {@link RocksDbDataRegionName}.
 */
public class RocksDbDataRegionValidatorImpl implements Validator<RocksDbDataRegionName, String> {
    /** Static instance. */
    public static final RocksDbDataRegionValidatorImpl INSTANCE = new RocksDbDataRegionValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(RocksDbDataRegionName annotation, ValidationContext<String> ctx) {
        String dataRegion = ctx.getNewValue();

        StoragesView engineConfig = ctx.getNewRoot(StoragesConfiguration.KEY);

        assert engineConfig != null;

        if (engineConfig.profiles().get(dataRegion) != null
                && (engineConfig.profiles().get(dataRegion) instanceof RocksDbStorageProfileConfiguration)) {
            ctx.addIssue(new ValidationIssue(
                    ctx.currentKey(),
                    String.format(
                            "Unable to find data region '%s' in configuration '%s'",
                            dataRegion,
                            StoragesConfiguration.KEY
                    )));
        }
    }

//    private static boolean contains(RocksDbStorageEngineView engineConfig, String dataRegion) {
//        return engineConfig.defaultRegion().name().equals(dataRegion) || engineConfig.regions().get(dataRegion) != null;
//    }
}
