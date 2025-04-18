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

package org.apache.ignite.internal.testframework.failure;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import org.apache.ignite.internal.testframework.log4j2.Log4jUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit extension that allows to configure FailureManager-related aspects, including usage of {@link MuteFailureManagerLogging}
 * annotations.
 *
 * <p>Should be used in {@link ExtendWith}.
 *
 * @see MuteFailureManagerLogging
 * @see ExtendWith
 */
public class FailureManagerExtension implements
        BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(FailureManagerExtension.class);

    private static final String FAILURE_MANAGER_CLASS_NAME = "org.apache.ignite.internal.failure.FailureManager";

    /** {@inheritDoc} */
    @Override
    public void beforeAll(ExtensionContext context) {
        boolean annotated = false;
        for (Class<?> cls = context.getRequiredTestClass(); cls != null; cls = cls.getSuperclass()) {
            if (cls.getAnnotation(MuteFailureManagerLogging.class) != null) {
                annotated = true;
                break;
            }
        }

        if (annotated) {
            rememberOldLevelAndMuteLogger(context);
        }
    }

    private static void rememberOldLevelAndMuteLogger(ExtensionContext context) {
        Log4jUtils.waitTillConfigured();

        Logger logger = failureManagerLogger();

        context.getStore(NAMESPACE).put(context.getUniqueId(), logger.getLevel());

        Configurator.setLevel(logger, Level.OFF);
    }

    private static Logger failureManagerLogger() {
        return LogManager.getLogger(FAILURE_MANAGER_CLASS_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext context) {
        restoreOldLevelIfSaved(context);
    }

    private static void restoreOldLevelIfSaved(ExtensionContext context) {
        Level oldLevel = context.getStore(NAMESPACE).remove(context.getUniqueId(), Level.class);
        if (oldLevel != null) {
            Logger logger = failureManagerLogger();
            Configurator.setLevel(logger, oldLevel);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void beforeEach(ExtensionContext context) {
        if (context.getRequiredTestMethod().isAnnotationPresent(MuteFailureManagerLogging.class)) {
            rememberOldLevelAndMuteLogger(context);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) {
        restoreOldLevelIfSaved(context);
    }
}
