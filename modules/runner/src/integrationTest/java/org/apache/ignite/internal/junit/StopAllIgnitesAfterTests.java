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

package org.apache.ignite.internal.junit;

import com.google.auto.service.AutoService;
import java.util.ServiceLoader;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.app.IgnitionImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * This extension tries to do its best to stop all Ignite instances that were started in this JVM after a test
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
@AutoService(Extension.class)
public class StopAllIgnitesAfterTests implements AfterAllCallback {
    private static final IgniteLogger LOG = Loggers.forClass(StopAllIgnitesAfterTests.class);

    @Override
    public void afterAll(ExtensionContext context) {
        String testInstanceName = context.getTestClass().map(Class::getName).orElse("<unknown>");

        LOG.info("Trying to stop all Ignites in {}", testInstanceName);

        IgnitionImpl ignition = loadIgnitionService();

        ignition.stopAll();

        LOG.info("Stopped all Ignites in {}", testInstanceName);
    }

    private static IgnitionImpl loadIgnitionService() {
        ServiceLoader<?> ldr = ServiceLoader.load(Ignition.class, Thread.currentThread().getContextClassLoader());
        return (IgnitionImpl) ldr.iterator().next();
    }
}
