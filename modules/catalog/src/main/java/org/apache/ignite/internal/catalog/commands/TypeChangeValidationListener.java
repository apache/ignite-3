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

package org.apache.ignite.internal.catalog.commands;

import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Listener that gets invoked when column type change fails for some reason with the exact reason of the failure.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface TypeChangeValidationListener {
    /**
     * A listener that does nothing.
     */
    TypeChangeValidationListener NO_OP = (pattern, originalType, newType) -> {
        // No-op.
    };

    /**
     * Gets invoked on a validation failure.
     *
     * @param pattern String pattern that denotes the validation failure reason (placeholders are marked with {}).
     * @param originalType Original column type.
     * @param newType New column type.
     */
    void onFailure(String pattern, ColumnType originalType, @Nullable ColumnType newType);
}
