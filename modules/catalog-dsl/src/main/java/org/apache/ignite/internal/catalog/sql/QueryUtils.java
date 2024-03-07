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

package org.apache.ignite.internal.catalog.sql;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility class with helpful methods.
 */
class QueryUtils {

    /**
     * Maps an array to the list, applying a mapping function to each entry.
     *
     * @param in Input array.
     * @param mapper Mapping function.
     * @param <T> Input entry type.
     * @param <R> Output entry type.
     *
     * @return List of mapped entries.
     */
    static <T, R> List<R> mapArrayToList(T[] in, Function<T, R> mapper) {
        return Arrays.stream(in)
                .map(mapper)
                .collect(Collectors.toList());
    }

    /**
     * Checks that the {@link Integer} is not null and is greater than zero.
     *
     * @param n Integer object to check.
     * @return {@code true} iff the argument is not null and is greater than zero.
     */
    static boolean isGreaterThanZero(Integer n) {
        return n != null && n > 0;
    }

    /**
     * Converts comma-separated string to a list of strings, leading and trailing spaces are trimmed.
     *
     * @param commaSeparatedString Comma-separated string.
     * @return List of strings.
     */
    static List<String> splitByComma(String commaSeparatedString) {
        return Arrays.stream(commaSeparatedString.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
