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

package org.apache.ignite.internal.pagememory.inmemory;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.AbstractPageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link VolatilePageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class VolatilePageMemoryNoLoadSelfTest extends AbstractPageMemoryNoLoadSelfTest {
    @InjectConfiguration(polymorphicExtensions = UnsafeMemoryAllocatorConfigurationSchema.class)
    private VolatilePageMemoryDataRegionConfiguration dataRegionCfg;

    @BeforeEach
    void setUp() throws Exception {
        dataRegionCfg.change(c -> c.changeInitSize(MAX_MEMORY_SIZE).changeMaxSize(MAX_MEMORY_SIZE)).get(1, SECONDS);
    }

    /** {@inheritDoc} */
    @Override
    protected VolatilePageMemory memory() {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                dataRegionCfg,
                ioRegistry,
                PAGE_SIZE
        );
    }
}
