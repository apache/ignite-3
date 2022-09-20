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

package org.apache.ignite.internal.configuration.processor;

import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.joinSimpleName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.junit.jupiter.api.Test;

/**
 * Class for testing the {@link ConfigurationProcessorUtils}.
 */
public class ConfigurationProcessorUtilsTest {
    @Test
    void testSimpleName() {
        assertEquals("@Config", simpleName(Config.class));
    }

    @Test
    void testJoinSimpleName() {
        assertEquals("@Config", joinSimpleName(" and ", Config.class));
        assertEquals("@Config or @ConfigurationRoot", joinSimpleName(" or ", Config.class, ConfigurationRoot.class));
        assertEquals("", joinSimpleName(" or "));
    }
}
