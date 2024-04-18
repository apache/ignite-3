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

package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Base class for schema exceptions.
 */
public class SchemaException extends IgniteInternalException {
    /**
     * Constructor with error message.
     *
     * @param msg Message.
     */
    public SchemaException(String msg) {
        super(msg);
    }

    /**
     * Constructor with error message and cause.
     *
     * @param msg   Message.
     * @param cause Cause.
     */
    public SchemaException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates a new exception with the given error code and detail message.
     *
     * @param code Full error code.
     * @param message Detail message.
     */
    public SchemaException(int code, String message) {
        super(code, message);
    }
}
