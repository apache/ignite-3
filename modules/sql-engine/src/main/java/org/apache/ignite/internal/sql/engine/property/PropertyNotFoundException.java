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

package org.apache.ignite.internal.sql.engine.property;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

/**
 * Exception that is thrown by {@link SqlProperties} if given property is not found.
 *
 * @see SqlProperties
 */
public class PropertyNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 4651104853616619936L;

    /** Constructs the exception. */
    PropertyNotFoundException(Property<?> prop) {
        super(format("Property '{}' not found", prop.name));
    }
}
