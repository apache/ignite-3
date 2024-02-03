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

package org.apache.ignite.internal.util;

/**
 * Code to work with Java language assertions (those that relate to the {@code assert} keyword).
 */
public class JavaAssertions {
    /**
     * Returns {@code true} iff Java assertions are enabled for the current class at the moment of invocation.
     */
    @SuppressWarnings({"NestedAssignment", "AssertWithSideEffects"})
    public static boolean enabled() {
        boolean enabled = false;

        // We deliberately use assert with side effect (and nested assignment) to find out whether assertions are enabled.
        assert (enabled = true);

        return enabled;
    }
}
