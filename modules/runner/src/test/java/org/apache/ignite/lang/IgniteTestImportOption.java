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

package org.apache.ignite.lang;

import static com.tngtech.archunit.core.importer.ImportOption.Predefined.ONLY_INCLUDE_TESTS;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import java.util.regex.Pattern;

public class IgniteTestImportOption implements ImportOption {
    private final Pattern integrationTestPattern = Pattern.compile(".*/build/classes/([^/]+/)integrationTest/.*");

    /** {@inheritDoc} */
    @Override
    public boolean includes(Location location) {
        return ONLY_INCLUDE_TESTS.includes(location) || location.matches(integrationTestPattern);
    }
}
