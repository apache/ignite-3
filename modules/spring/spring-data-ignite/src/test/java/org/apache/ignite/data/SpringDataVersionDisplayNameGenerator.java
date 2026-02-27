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

package org.apache.ignite.data;

import org.junit.jupiter.api.DisplayNameGenerator;

/**
 * Custom display name generator that appends Spring Data version to test class names.
 */
public class SpringDataVersionDisplayNameGenerator extends DisplayNameGenerator.Standard {

    private static final String VERSION_PROPERTY = "spring.data.version";

    @Override
    public String generateDisplayNameForClass(Class<?> testClass) {
        String version = System.getProperty(VERSION_PROPERTY, "unknown");
        return super.generateDisplayNameForClass(testClass) + " (Spring Data " + version + ")";
    }

    @Override
    public String generateDisplayNameForNestedClass(Class<?> nestedClass) {
        String version = System.getProperty(VERSION_PROPERTY, "unknown");
        return super.generateDisplayNameForNestedClass(nestedClass) + " (Spring Data " + version + ")";
    }
}
