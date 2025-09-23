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

package org.apache.ignite.internal.failure;

import java.util.UUID;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Failure context contains information about failure type and exception if applicable.
 * This information could be used for appropriate handling of the failure.
 */
public class FailureContext {
    /** Type. */
    private final FailureType type;

    /** Error. */
    private final @Nullable Throwable err;

    /** Message describing the failure. */
    private final String message;

    /** Unique identifier of this failure context. */
    private final UUID id = UUID.randomUUID();

    /**
     * Creates instance of {@link FailureContext} of {@link FailureType#CRITICAL_ERROR} type.
     *
     * @param err Exception.
     */
    public FailureContext(Throwable err) {
        this(FailureType.CRITICAL_ERROR, err);
    }

    /**
     * Creates instance of {@link FailureContext} corresponding to an unknown failure.
     *
     * @param type Failure type.
     * @param err Exception.
     */
    public FailureContext(FailureType type, Throwable err) {
        this(type, err, "Unknown error");
    }

    /**
     * Creates instance of {@link FailureContext} of {@link FailureType#CRITICAL_ERROR} type.
     *
     * @param err Exception.
     * @param message Message describing the failure.
     */
    public FailureContext(Throwable err, String message) {
        this(FailureType.CRITICAL_ERROR, err, message);
    }

    /**
     * Creates instance of {@link FailureContext}.
     *
     * @param type Failure type.
     * @param err Exception. Might be {@code null} if no exception is available.
     * @param message Message describing the failure.
     */
    public FailureContext(FailureType type, @Nullable Throwable err, String message) {
        this.type = type;
        this.err = err;
        this.message = message;
    }

    /**
     * Returns the failure type.
     *
     * @return Failure type.
     */
    public FailureType type() {
        return type;
    }

    /**
     * Returns the exception.
     *
     * @return Exception or {@code null}.
     */
    @Nullable
    public Throwable error() {
        return err;
    }

    /**
     * Returns the message.
     *
     * @return Message describing the failure.
     */
    public String message() {
        return message;
    }

    /**
     * Returns the unique identifier of this failure context.
     *
     * @return Unique identifier.
     */
    public UUID id() {
        return id;
    }

    @Override public String toString() {
        return S.toString(FailureContext.class, this);
    }
}
