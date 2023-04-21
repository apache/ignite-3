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

import com.tngtech.archunit.core.importer.Location;
import java.nio.file.Path;
import java.util.Set;

/**
 * Provide location providers for arch tests.
 */
public class LocationProvider {

    /**
     * Location provider for a root. Include all modules.
     */
    public static class RootLocationProvider implements com.tngtech.archunit.junit.LocationProvider {
        @Override
        public Set<Location> get(Class<?> testClass) {
            // ignite-3/modules
            Path modulesRoot = Path.of("").toAbsolutePath().getParent();

            return Set.of(Location.of(modulesRoot));
        }
    }
}
