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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.lang.ErrorGroups.Catalog.VALIDATION_ERR;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;

/**
 * Catalog validation exception.
 */
public class CatalogValidationException extends IgniteInternalException {
    private static final long serialVersionUID = 2692301541251354006L;

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public CatalogValidationException(String message) {
        super(VALIDATION_ERR, message);
    }

    /**
     * Constructor.
     *
     * @param messagePattern Error message pattern.
     * @param params Error message params.
     * @see IgniteStringFormatter#format(String, Object...)
     */
    public CatalogValidationException(String messagePattern, Object... params) {
        super(VALIDATION_ERR, messagePattern, params);
    }

    /**
     * Constructor.
     *
     * @param messagePattern Error message pattern.
     * @param cause Non-null throwable cause.
     * @param params Error message params.
     * @see IgniteStringFormatter#format(String, Object...)
     */
    CatalogValidationException(String messagePattern, Throwable cause, Object... params) {
        super(VALIDATION_ERR, messagePattern, cause, params);
    }
}
