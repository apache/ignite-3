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

package org.apache.ignite.internal.configuration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for SuperRoot nodeCreator behavior.
 */
class SuperRootNodeCreatorTest extends BaseIgniteAbstractTest {

    /**
     * Tests that SuperRoot with key-checking nodeCreator rejects unknown keys.
     */
    @Test
    public void constructShouldThrowForUnknownRootKey() {
        var cgen = new ConfigurationAsmGenerator();
        cgen.compileRootSchema(TestConfigurationSchema.class, Map.of(), Map.of());

        // Use real RootKey from TestConfiguration
        var rootKey = TestConfiguration.KEY;

        // Create SuperRoot with nodeCreator that checks the key (the fixed pattern)
        SuperRoot superRoot = new SuperRoot(s ->
                s.equals(rootKey.key())
                        ? new RootInnerNode(rootKey, cgen.instantiateNode(TestConfigurationSchema.class))
                        : null
        );

        // For known key - construct should work
        assertDoesNotThrow(
                () -> superRoot.construct(rootKey.key(), ConfigurationUtil.EMPTY_CFG_SRC, true),
                "construct() should succeed for known key"
        );

        // For unknown key - construct should throw NoSuchElementException
        assertThrows(NoSuchElementException.class,
                () -> superRoot.construct("unknownKey", ConfigurationUtil.EMPTY_CFG_SRC, true),
                "construct() should throw NoSuchElementException for unknown key"
        );
    }
}
