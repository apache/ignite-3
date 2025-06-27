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
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
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
            muteLoggerAndMemorizeMute(context);
        }
    }

    private static void muteLoggerAndMemorizeMute(ExtensionContext context) {
        Log4jUtils.waitTillConfigured();

        muteLogger();

        context.getStore(NAMESPACE).put(context.getUniqueId(), Boolean.TRUE);
    }

    private static void muteLogger() {
        LoggerContext rootLoggerContext = getRootLoggerContext();

        // We add a non-additive logger to make sure that logging configuration inherited from root logger is not used here.
        LoggerConfig loggerConfig = new LoggerConfig(FAILURE_MANAGER_CLASS_NAME, Level.INFO, false);
        rootLoggerContext.getConfiguration().addLogger(FAILURE_MANAGER_CLASS_NAME, loggerConfig);

        rootLoggerContext.updateLoggers();
    }

    private static LoggerContext getRootLoggerContext() {
        Logger rootLogger = (Logger) LogManager.getRootLogger();
        return rootLogger.getContext();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        unmuteIfMuted(context);
    }

    private static void unmuteIfMuted(ExtensionContext context) {
        Boolean wasMutedMarker = context.getStore(NAMESPACE).remove(context.getUniqueId(), Boolean.class);

        if (wasMutedMarker != null && wasMutedMarker) {
            unmute();
        }
    }

    private static void unmute() {
        LoggerContext rootLoggerContext = getRootLoggerContext();
        rootLoggerContext.getConfiguration().removeLogger(FAILURE_MANAGER_CLASS_NAME);
        rootLoggerContext.updateLoggers();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        if (context.getRequiredTestMethod().isAnnotationPresent(MuteFailureManagerLogging.class)) {
            muteLoggerAndMemorizeMute(context);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        unmuteIfMuted(context);
    }
}
