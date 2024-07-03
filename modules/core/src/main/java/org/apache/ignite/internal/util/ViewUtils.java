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

import java.util.Collection;
import java.util.Objects;

/**
 * Table view utilities.
 */
public final class ViewUtils {
    /**
     * Checks that given keys collection isn't null and there is no a null-value key.
     *
     * @param keys Given keys collection.
     * @param <K> Keys type.
     * @throws NullPointerException In case if the collection null either any key is null.
     */
    public static <K> void checkKeysForNulls(Collection<K> keys) {
        Objects.requireNonNull(keys, "keys");

        for (K key : keys) {
            Objects.requireNonNull(key, "key");
        }
    }

    private ViewUtils() { }
}
