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

import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods used for cluster's named objects: schemas, tables, columns, indexes, etc.
 */
public final class IgniteNameUtils {
    /** No instance methods. */
    private IgniteNameUtils() {
    }

    /**
     * Parse simple name: unquote name or cast to upper case not-quoted name.
     *
     * @param name String to parse object name.
     * @return Unquoted name or name is cast to upper case. "tbl0" -&gt; "TBL0", "\"Tbl0\"" -&gt; "Tbl0".
     */
    public static String parseSimpleName(String name) {
        if (StringUtils.nullOrEmpty(name)) {
            return name;
        }

        var tokenizer = new Tokenizer(name);

        String parsedName = tokenizer.nextToken();

        if (tokenizer.nextToken() != null) {
            throw new IgniteInternalException("Fully qualified name is not expected [name=" + name + "]");
        }

        return parsedName;
    }

    public static String canonicalName(String schemaName, String objectName) {
        return quote(schemaName) + '.' + quote(objectName);
    }

    /**
     * Wraps the given name with double quotes, e.g. "myColumn" -&gt; "\"myColumn\""
     *
     * @param name Object name.
     * @return Quoted object name.
     */
    public static String quote(String name) {
        return "\"" + name + "\"";
    }

    private static class Tokenizer {
        private int currentPosition;
        private final String source;

        public Tokenizer(String source) {
            this.source = source;
        }

        public @Nullable String nextToken() {
            if (currentPosition >= source.length()) {
                return null;
            }

            boolean quoted = source.charAt(currentPosition) == '"';

            if (quoted) {
                currentPosition++;
            }

            int start = currentPosition;
            StringBuilder sb = new StringBuilder();

            for (; currentPosition < source.length(); currentPosition++) {
                if (currentChar() == '"') {
                    if (hasNextChar() && nextChar() == '"') {  // quote is escaped
                        sb.append(source, start, currentPosition + 1);

                        currentPosition += 2;
                        start = currentPosition;

                        continue;
                    } else if (quoted && (!hasNextChar() || nextChar() == '.')) {
                        // looks like we just found a closing quote
                        sb.append(source, start, currentPosition);

                        currentPosition += 2;

                        return sb.toString();
                    }
                } else if (!quoted && currentChar() == '.') {
                    sb.append(source, start, currentPosition);

                    currentPosition++;

                    return sb.toString().toUpperCase();
                }

                // throw new IgniteInternalException("Malformed name [name=" + source + ", pos=" + currentPosition + "]");
            }

            return source.substring(start).toUpperCase();
        }

        private char currentChar() {
            return source.charAt(currentPosition);
        }

        private boolean hasNextChar() {
            return currentPosition + 1 < source.length();
        }

        private char nextChar() {
            return source.charAt(currentPosition + 1);
        }
    }
}