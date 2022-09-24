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

package org.apache.ignite.internal.sql.engine.property;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link PropertiesHelper} class.
 */
class PropertiesHelperTest {
    private static final String NON_STATIC_PROP_NAME = "non_static_prop";

    public static class TestProps {
        public static final Property<Long> LONG_PROP = new Property<>("long_prop", Long.class);
        public static final Property<String> STRING_PROP = new Property<>("string_prop", String.class);
        private static final Property<String> PRIVATE_PROP = new Property<>("private_prop", String.class);
        static final Property<String> PROTECTED_PROP = new Property<>("protected_prop", String.class);

        @SuppressWarnings("unused")
        public final Property<String> nonStaticProp = new Property<>(NON_STATIC_PROP_NAME, String.class);
    }

    @Test
    public void buildPropMapFromClass() {
        var propMap = PropertiesHelper.createPropsByNameMap(TestProps.class);

        assertFalse(propMap.isEmpty());

        {
            var knownPublicProps = List.of(
                    TestProps.LONG_PROP,
                    TestProps.STRING_PROP
            );

            for (var expProp : knownPublicProps) {
                var actProp = propMap.get(expProp.name);

                assertEquals(expProp, actProp);
                assertEquals(expProp.cls, actProp.cls);
            }
        }

        {
            var omittedProps = List.of(
                    TestProps.PRIVATE_PROP,
                    TestProps.PROTECTED_PROP
            );

            for (var prop : omittedProps) {
                assertNull(propMap.get(prop.name));
            }
        }

        {
            assertNull(propMap.get(NON_STATIC_PROP_NAME));
        }
    }
}
