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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.ClusterConfiguration.configOverrides;
import static org.apache.ignite.internal.ClusterConfiguration.containsOverrides;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.applyOverridesToConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ConfigOverrideTest {
    @Test
    void noOverrides(TestInfo testInfo) {
        assertThat(containsOverrides(testInfo, 0), is(false));
    }

    @Test
    @ConfigOverride(name = "key", value = "value")
    void overrideAllNodes(TestInfo testInfo) {
        assertThat(containsOverrides(testInfo, 0), is(true));
        assertThat(containsOverrides(testInfo, 1), is(true));
        String config = "key=foo,key1=bar";
        String config0 = applyOverridesToConfig(config, configOverrides(testInfo, 0));
        String config1 = applyOverridesToConfig(config, configOverrides(testInfo, 1));
        assertThat(config0, is("key=value,key1=bar"));
        assertThat(config1, is("key=value,key1=bar"));
    }

    @Test
    @ConfigOverride(name = "key", value = "value", nodeIndex = 1)
    void overrideSingleNode(TestInfo testInfo) {
        assertThat(containsOverrides(testInfo, 0), is(false));
        assertThat(containsOverrides(testInfo, 1), is(true));
        String config = "key=foo,key1=bar";
        String config0 = applyOverridesToConfig(config, configOverrides(testInfo, 0));
        String config1 = applyOverridesToConfig(config, configOverrides(testInfo, 1));
        assertThat(config0, is("key=foo,key1=bar"));
        assertThat(config1, is("key=value,key1=bar"));
    }

    @Test
    @ConfigOverride(name = "foo", value = "bar")
    @ConfigOverride(name = "baz", value = "qux")
    @ConfigOverride(name = "foo", value = "value")
    void multipleOverridesOrder(TestInfo testInfo) {
        assertThat(containsOverrides(testInfo, 0), is(true));
        String config = "foo=nil";
        String config0 = applyOverridesToConfig(config, configOverrides(testInfo, 0));
        assertThat(config0, is("foo=value, baz : qux"));
    }

    @ConfigOverride(name = "key", value = "value")
    @ConfigOverride(name = "foo", value = "bar")
    @ConfigOverride(name = "baz", value = "qux")
    static class ParentAnnotation {
    }

    @Nested
    class ChildTest extends ParentAnnotation {
        @Test
        void parentAnnotation(TestInfo testInfo) {
            assertThat(containsOverrides(testInfo, 0), is(true));
            String config = "key=foo";
            String config0 = applyOverridesToConfig(config, configOverrides(testInfo, 0));
            assertThat(config0, is("key=value, foo : bar, baz : qux"));
        }

        @Test
        @ConfigOverride(name = "key", value = "value1")
        void parentAnnotationOverride(TestInfo testInfo) {
            assertThat(containsOverrides(testInfo, 0), is(true));
            String config = "key=foo";
            String config0 = applyOverridesToConfig(config, configOverrides(testInfo, 0));
            assertThat(config0, is("key=value1, foo : bar, baz : qux"));
        }
    }
}
