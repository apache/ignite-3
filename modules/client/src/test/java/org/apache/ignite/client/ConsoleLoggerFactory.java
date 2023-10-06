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

package org.apache.ignite.client;

import java.lang.System.Logger;
import java.util.ResourceBundle;
import java.util.function.Supplier;
import org.apache.ignite.lang.LoggerFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Logger factory that logs to the console.
 */
public class ConsoleLoggerFactory implements LoggerFactory {
    private final ConsoleLogger logger;

    ConsoleLoggerFactory(String factoryName) {
        this.logger = new ConsoleLogger(factoryName);
    }

    @Override
    public Logger forName(String name) {
        return logger;
    }

    private static class ConsoleLogger implements System.Logger {
        private final String name;

        ConsoleLogger(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        @Override
        public void log(Level level, String msg) {
            captureLog(msg, level, null);
        }

        @Override
        public void log(Level level, Supplier<String> msgSupplier) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, Object obj) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, String msg, Throwable thrown) {
            captureLog(msg, level, null);
        }

        @Override
        public void log(Level level, Supplier<String> msgSupplier, Throwable thrown) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, String format, Object... params) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            throw new AssertionError("Should not be called");
        }

        private static void captureLog(String msg, Level level, @Nullable Throwable thrown) {
            System.out.println("[" + level + "] " + msg);

            if (thrown != null) {
                thrown.printStackTrace(System.out);
            }
        }
    }
}
