/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.util;

import org.jetbrains.annotations.Nullable;

/**
 * Class containing useful methods for working with strings.
 */
public final class StringUtils {
    private StringUtils() {
    }

    /**
     * Tests if given string is {@code null} or empty.
     *
     * @param s String to test.
     * @return Whether or not the given string is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Tests if given string is {@code null} or {@link String#isBlank}.
     *
     * @param s String to test.
     * @return Whether or not the given string is {@code null} or blank.
     */
    public static boolean nullOrBlank(@Nullable String s) {
        return s == null || s.isBlank();
    }
}
