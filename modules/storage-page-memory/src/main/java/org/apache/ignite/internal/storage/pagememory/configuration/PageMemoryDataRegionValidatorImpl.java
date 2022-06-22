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

package org.apache.ignite.internal.storage.pagememory.configuration;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineView;

/**
 * Implementing a validator for {@link PageMemoryDataRegionName}.
 */
public class PageMemoryDataRegionValidatorImpl implements Validator<PageMemoryDataRegionName, String> {
    /** Static instance. */
    public static final PageMemoryDataRegionValidatorImpl INSTANCE = new PageMemoryDataRegionValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(PageMemoryDataRegionName annotation, ValidationContext<String> ctx) {
        String dataRegion = ctx.getNewValue();

        // TODO: IGNITE-17149 поменять логику и покрыть тестами

        PageMemoryStorageEngineView engineConfig = ctx.getNewRoot(PageMemoryStorageEngineConfiguration.KEY);

        assert engineConfig != null;

        if (!contains(engineConfig, dataRegion)) {
            ctx.addIssue(new ValidationIssue(String.format(
                    "Unable to find data region '%s' in configuration '%s'",
                    dataRegion,
                    PageMemoryStorageEngineConfiguration.KEY
            )));
        }
    }

    private static boolean contains(PageMemoryStorageEngineView engineConfig, String dataRegion) {
        return engineConfig.defaultRegion().name().equals(dataRegion) || engineConfig.regions().get(dataRegion) != null;
    }
}
