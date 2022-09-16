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

import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineView;

/**
 * Implementing a validator for {@link PageMemoryDataRegionName}.
 */
public class PageMemoryDataRegionValidatorImpl implements Validator<PageMemoryDataRegionName, String> {
    /** Static instance. */
    public static final PageMemoryDataRegionValidatorImpl INSTANCE = new PageMemoryDataRegionValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(PageMemoryDataRegionName annotation, ValidationContext<String> ctx) {
        String dataRegionName = ctx.getNewValue();

        Object newOwner = ctx.getNewOwner();

        if (newOwner instanceof InnerNode) {
            newOwner = ((InnerNode) newOwner).specificNode();
        }

        if (newOwner instanceof VolatilePageMemoryDataStorageView) {
            VolatilePageMemoryStorageEngineView engineConfig = ctx.getNewRoot(VolatilePageMemoryStorageEngineConfiguration.KEY);

            assert engineConfig != null;

            if (!contains(engineConfig, dataRegionName)) {
                ctx.addIssue(unableToFindDataRegionIssue(
                        ctx.currentKey(),
                        dataRegionName,
                        VolatilePageMemoryStorageEngineConfiguration.KEY)
                );
            }
        } else if (newOwner instanceof PersistentPageMemoryDataStorageView) {
            PersistentPageMemoryStorageEngineView engineConfig = ctx.getNewRoot(PersistentPageMemoryStorageEngineConfiguration.KEY);

            assert engineConfig != null;

            if (!contains(engineConfig, dataRegionName)) {
                ctx.addIssue(unableToFindDataRegionIssue(
                        ctx.currentKey(),
                        dataRegionName,
                        PersistentPageMemoryStorageEngineConfiguration.KEY)
                );
            }
        } else {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), String.format("Unknown data storage '%s'", newOwner)));
        }
    }

    private static boolean contains(VolatilePageMemoryStorageEngineView engineConfig, String dataRegionName) {
        return engineConfig.defaultRegion().name().equals(dataRegionName) || engineConfig.regions().get(dataRegionName) != null;
    }

    private static boolean contains(PersistentPageMemoryStorageEngineView engineConfig, String dataRegionName) {
        return engineConfig.defaultRegion().name().equals(dataRegionName) || engineConfig.regions().get(dataRegionName) != null;
    }

    private static ValidationIssue unableToFindDataRegionIssue(String validationKey, String dataRegionName, RootKey<?, ?> rootKey) {
        return new ValidationIssue(
                validationKey,
                String.format("Unable to find data region '%s' in configuration '%s'", dataRegionName, rootKey)
        );
    }
}
