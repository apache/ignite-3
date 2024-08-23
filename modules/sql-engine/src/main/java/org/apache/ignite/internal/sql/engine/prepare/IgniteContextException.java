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

package org.apache.ignite.internal.sql.engine.prepare;

import org.apache.calcite.runtime.CalciteContextException;

/**
 * Exception which contains information about the textual context of the causing exception.
 */
class IgniteContextException extends CalciteContextException {

    private static final long serialVersionUID = 994259246365437614L;

    private final String message;

    /**
     * Creates a new IgniteContextException object.
     *
     * @param message error message
     * @param cause underlying cause, must not be null
     */
    IgniteContextException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        // We override this method since CalciteContextException::getMessage
        // concatenates both cause::message and this::message,
        // and we already have a concatenated message in this::message.
        return message;
    }
}
