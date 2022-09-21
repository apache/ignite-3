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

package org.apache.ignite.configuration.validation;

import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Class that stores information about issues found during the configuration validation.
 */
public class ValidationIssue {
    /** Configuration key. */
    @IgniteToStringInclude
    private final String key;

    /** Message. */
    @IgniteToStringInclude
    private final String message;

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public ValidationIssue(String key, String message) {
        this.key = key;
        this.message = message;
    }

    /**
     * Returns configuration key that did not pass the validation.
     *
     * @return Configuration key.
     */
    public String key() {
        return key;
    }

    /**
     * Returns error message.
     *
     * @return Error message.
     */
    public String message() {
        return message;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ValidationIssue.class, this);
    }
}
