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

package org.apache.ignite.internal.configuration.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Class of checkers for possible system configuration properties compatibility issues. It should help if e.g. a property name was silently
 * changed in code, but internal documentation changes were omitted.
 */
public final class SystemConfigurationPropertyCompatibilityChecker {
    /**
     * Asserts that the system configuration property that is declared as given constant field name wasn't changed.
     *
     * @param propertyConstantFieldName The property's constant field name to address if check failed.
     * @param propertyExpectedValue The property's expected value.
     * @param propertyActualValue The property's actual value.
     */
    public static void checkSystemConfigurationPropertyNameWasNotChanged(
            String propertyConstantFieldName,
            String propertyActualValue,
            String propertyExpectedValue
    ) {
        assertEquals(
                propertyExpectedValue,
                propertyActualValue,
                "System configuration property "
                        + propertyConstantFieldName + " was changed from \""
                        + propertyExpectedValue + "\" to \""
                        + propertyConstantFieldName + ", compatibility and internal documentation consistency might be lost."
        );
    }
}
