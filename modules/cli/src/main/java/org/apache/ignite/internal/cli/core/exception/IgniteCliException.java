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

package org.apache.ignite.internal.cli.core.exception;

/**
 * Top level runtime exception for throwing the error message to user.
 */
public class IgniteCliException extends RuntimeException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new instance of {@code IgniteCliException} with the given {@code msg}.
     *
     * @param msg Detailed message.
     */
    public IgniteCliException(String msg) {
        super(msg);
    }

    /**
     * Creates a new instance of {@code IgniteCliException} with the given {@code msg} and {@code cause}.
     *
     * @param msg   Detailed message.
     * @param cause Cause.
     */
    public IgniteCliException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
