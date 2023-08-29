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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe component implementing string interning without using the standard {@link String#intern()}
 * (avoiding its data structures pollution).
 *
 * @see String#intern()
 */
public class ConcurrentStringInternizer {
    /** Mapping from strings to their internized representations. */
    private final Map<String, String> stringToIntern = new ConcurrentHashMap<>();

    /**
     * Internizes the given string. That is, if this method is called a few times on the same internizer
     * with equal (but not necessarily same) instances as an argument, it will return the same String object
     * on each invocation.
     *
     * @param string String to internize.
     * @return Internized representation of the given string.
     */
    public String intern(String string) {
        return stringToIntern.computeIfAbsent(string, key -> key);
    }
}
