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

package org.apache.ignite.internal.tostring;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.junit.jupiter.api.Test;

class IgniteToStringBuilderTest {
    @Test
    void includesFieldsOfAllVisibilityLevels() {
        WithFieldsOfAllVisibilities object = new WithFieldsOfAllVisibilities();

        String str = IgniteToStringBuilder.toString(object);

        assertThat(str, containsString("privateField=1"));
        assertThat(str, containsString("protectedField=2"));
        assertThat(str, containsString("packagePrivateField=3"));
        assertThat(str, containsString("publicField=4"));
    }

    @SuppressWarnings({"FieldMayBeStatic", "unused"})
    private static class WithFieldsOfAllVisibilities {
        private final String privateField = "1";
        protected final String protectedField = "2";
        final String packagePrivateField = "3";
        public final String publicField = "4";
    }
}
