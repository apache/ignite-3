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

package org.apache.ignite.internal.pagememory.inmemory;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.AbstractPageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link VolatilePageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class VolatilePageMemoryNoLoadSelfTest extends AbstractPageMemoryNoLoadSelfTest {
    @InjectConfiguration(
            polymorphicExtensions = VolatilePageMemoryProfileConfigurationSchema.class,
            value = "mock = {"
            + "engine=aimem, "
            + "initSize=" + MAX_MEMORY_SIZE
            + ", maxSize=" + MAX_MEMORY_SIZE
            + "}"
    )
    private StorageProfileConfiguration storageProfileCfg;

    /** {@inheritDoc} */
    @Override
    protected VolatilePageMemory memory() {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                (VolatilePageMemoryProfileConfiguration) fixConfiguration(storageProfileCfg),
                ioRegistry,
                PAGE_SIZE
        );
    }
}
