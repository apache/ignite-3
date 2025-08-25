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

package org.apache.ignite.internal.lang;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.UUID;
import org.apache.ignite.tx.RetriableTransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * This exception is used to indicate that a component (like replication group client) is stopping (already stopped) for some reason.
 * This is different from {@link NodeStoppingException} as {@link ComponentStoppingException} might mean that just the component is stopped,
 * not the whole node.
 */
public class ComponentStoppingException extends IgniteInternalCheckedException implements RetriableTransactionException {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /**
     * Creates an empty component stopping exception.
     */
    public ComponentStoppingException() {
        super(INTERNAL_ERR, "Operation has been cancelled (component is stopping).");
    }

    /**
     * Creates a new exception with the given error message.
     *
     * @param msg Error message.
     */
    public ComponentStoppingException(String msg) {
        super(INTERNAL_ERR, msg);
    }

    /**
     * Creates a new component stopping exception with the given throwable as a cause and source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public ComponentStoppingException(Throwable cause) {
        super(INTERNAL_ERR, cause);
    }

    /**
     * Creates a new component stopping exception with the given error message and optional nested exception.
     *
     * @param msg                Error message.
     * @param cause              Optional nested exception (can be {@code null}).
     * @param writableStackTrace Whether or not the stack trace should be writable.
     */
    public ComponentStoppingException(String msg, @Nullable Throwable cause, boolean writableStackTrace) {
        super(UUID.randomUUID(), INTERNAL_ERR, msg, cause, writableStackTrace);
    }

    /**
     * Creates a new component stopping exception with the given error message and optional nested exception.
     *
     * @param msg   Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ComponentStoppingException(String msg, @Nullable Throwable cause) {
        super(INTERNAL_ERR, msg, cause);
    }
}
