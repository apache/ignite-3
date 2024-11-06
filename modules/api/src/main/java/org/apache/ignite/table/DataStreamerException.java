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

package org.apache.ignite.table;

import java.util.Set;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Represents an exception that is thrown during data streaming. Includes information about failed items.
 */
public final class DataStreamerException extends IgniteException {
    private final Set<Object> failedItems;

    public DataStreamerException(int code, String message, @Nullable Set<Object> failedItems) {
        super(code, message);

        this.failedItems = failedItems;
    }

    /**
     * Gets the set of failed items.
     *
     * @return Set of failed items.
     */
    public Set<Object> failedItems() {
        return failedItems;
    }
}
