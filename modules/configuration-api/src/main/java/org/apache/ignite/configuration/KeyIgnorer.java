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

package org.apache.ignite.configuration;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Determines if key should be ignored while processing configuration. */
@FunctionalInterface
public interface KeyIgnorer {
    /** Returns true if key should be ignored. */
    boolean shouldIgnore(String key);

    /** Creates a key ignorer that uses specified deletedPrefixes. */
    static KeyIgnorer fromDeletedPrefixes(Collection<String> deletedPrefixes) {
        Collection<Pattern> patterns = deletedPrefixes.stream()
                .map(deletedKey -> deletedKey.replace(".", "\\.").replace("*", "[^.]*") + "(\\..*)?")
                .map(Pattern::compile)
                .collect(Collectors.toList());

        return key -> patterns.stream().anyMatch(pattern -> pattern.matcher(key).matches());
    }
}
