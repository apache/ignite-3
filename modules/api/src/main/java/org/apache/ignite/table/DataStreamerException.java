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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Set;
import org.apache.ignite.lang.IgniteException;

/**
 * Represents an exception that is thrown during data streaming. Includes information about failed items.
 */
public final class DataStreamerException extends IgniteException {
    private static final long serialVersionUID = -7516813059448101081L;

    private final Set<?> failedItems;

    public DataStreamerException(Set<?> failedItems, Throwable cause) {
        super(getCode(cause), cause.getMessage(), cause);

        this.failedItems = failedItems;
    }

    /**
     * Gets the set of failed items.
     *
     * @return Set of failed items.
     */
    public Set<?> failedItems() {
        return failedItems;
    }

    private static int getCode(Throwable cause) {
        if (cause instanceof IgniteException) {
            IgniteException e = (IgniteException) cause;

            return e.code();
        }

        if (cause.getCause() instanceof IgniteException) {
            IgniteException e = (IgniteException) cause.getCause();

            return e.code();
        }

        return INTERNAL_ERR;
    }
}
