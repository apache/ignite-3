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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class DeletedKeysFilter {
    /**
     * Filters out keys that match the deleted prefixes and returns the ignored keys.
     *
     * @param values The map of values to filter.
     * @param deletedPrefixes Patterns of prefixes, deleted from the source.
     * @return A collection of keys that were ignored.
     */
    static List<String> ignoreDeleted(
            Map<String, ? extends Serializable> values,
            Collection<Pattern> deletedPrefixes
    ) {
        if (deletedPrefixes.isEmpty()) {
            return List.of();
        }

        List<String> ignoredKeys = values.keySet().stream()
                .filter(key -> isDeleted(key, deletedPrefixes))
                .collect(Collectors.toList());

        for (String key : ignoredKeys) {
            values.remove(key);
        }

        return ignoredKeys;
    }

    private static boolean isDeleted(String key, Collection<Pattern> patterns) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(key).matches()) {
                return true;
            }
        }

        return false;
    }
}
