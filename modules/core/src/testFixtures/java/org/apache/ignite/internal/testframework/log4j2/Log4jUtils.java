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

package org.apache.ignite.internal.testframework.log4j2;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Utilities to work with log4j.
 */
public class Log4jUtils {
    private static final String INIT_COMPLETE_LOGGER = "InitComplete";

    /**
     * Waits till log4j finishes its standard configuration procedure (applying the config from XML file).
     * This should be called before we attempt any programmatic log4j reconfiguration to avoid races.
     */
    public static void waitTillConfigured() {
        boolean configured;
        try {
            configured = waitForCondition(() -> {
                LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                Configuration config = ctx.getConfiguration();
                LoggerConfig loggerConfig = config.getLoggerConfig(INIT_COMPLETE_LOGGER);
                // If the logger we get by name has the given name, then it has its own configuration, so the configuration is finished.
                return INIT_COMPLETE_LOGGER.equals(loggerConfig.getName());
            }, SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(
                configured,
                "Log4j standard configuration was not completed in time; is there a logger called " + INIT_COMPLETE_LOGGER + "?"
        );
    }
}
