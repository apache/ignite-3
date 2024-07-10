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

package org.apache.ignite;

import java.util.UUID;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Thrown when, during an attempt to execute a transactional operation, it turns out that the operation cannot be executed
 * because an incompatible schema change has happened.
 */
public class IncompatibleSchemaException extends IgniteException {
    /**
     * Constructs a new instance of IncompatibleSchemaException.
     *
     * @param traceId Trace ID.
     * @param code Error code.
     * @param message Error message.
     * @param cause The Throwable that is the cause of this exception (can be {@code null}).
     */
    public IncompatibleSchemaException(UUID traceId, int code, String message, @Nullable Throwable cause) {
        super(traceId, code, message, cause);
    }
}
