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

package org.apache.ignite.internal.testframework.junit;

import java.lang.reflect.Method;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * This extension tries to do it best to stop all Ignite instances that were started in this JVM after a test
 * suite finishes running (after-all).
 *
 * <p>This extension is designed to be
 * <a href="https://junit.org/junit5/docs/current/user-guide/#extensions-registration-automatic">automatically registered</a>
 * via META-INF/services/org.junit.jupiter.api.extension.Extension.
 * For this to work, system property {@code junit.jupiter.extensions.autodetection.enabled} must be set to {@code true}.
 * If the property is set (currently, this is done via Gradle build scripts), it is enough to add this module as a dependency
 * to make tests automatically register this extension, like this:
 *
 * <pre>
 * integrationTestImplementation(testFixtures(project(':ignite-core')))
 * </pre>
 */
public class StopAllIgnitesAfterTests implements AfterAllCallback {
    private static final String IGNITION_MANAGER_CLASS_NAME = "org.apache.ignite.IgnitionManager";

    private static final String IGNITION_IMPL_CLASS_NAME = "org.apache.ignite.internal.app.IgnitionImpl";

    private static final IgniteLogger LOG = Loggers.forClass(StopAllIgnitesAfterTests.class);

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        String testInstanceName = context.getTestClass().map(Class::getName).orElse("<unknown>");

        LOG.info("StopAllIgnitesAfterTests entered: {}", testInstanceName);

        // Try to stop all Ignite nodes via reflection to make sure that this extension does not break anything
        // even in modules where IgnitionManager is not available (so even for unit tests).

        Class<?> ignitionManagerClass = findClassByName(IGNITION_MANAGER_CLASS_NAME);

        if (ignitionManagerClass != null && isIgnitionImplAvailable()) {
            LOG.info("Trying to stop all Ignites in {}", testInstanceName);

            Method stopAllMethod = ignitionManagerClass.getMethod("stopAll");

            stopAllMethod.invoke(null);

            LOG.info("Stopped all Ignites in {}", testInstanceName);
        }
    }

    private static boolean isIgnitionImplAvailable() {
        Class<?> ignitionImplClass = findClassByName(IGNITION_IMPL_CLASS_NAME);

        return ignitionImplClass != null;
    }

    @Nullable
    private static Class<?> findClassByName(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
