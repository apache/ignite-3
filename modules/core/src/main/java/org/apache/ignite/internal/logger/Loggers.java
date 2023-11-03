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

package org.apache.ignite.internal.logger;

import java.util.Objects;
import org.apache.ignite.lang.LoggerFactory;

/**
 * This class contains different static factory methods to create an instance of logger.
 */
public final class Loggers {
    /**
     * Creates logger for given class with system logger as a backend.
     *
     * <p>Note: this method should be used to create a server-side logger since user might provide a custom backend for
     * every particular client instance.
     *
     * @param cls The class for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls) {
        return forName(Objects.requireNonNull(cls, "cls").getName());
    }

    /**
     * Creates logger for given class with backend created by the given loggerFactory.
     *
     * @param cls The class for a logger.
     * @param loggerFactory Logger factory to create backend for the logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls, LoggerFactory loggerFactory) {
        return forName(Objects.requireNonNull(cls, "cls").getName(), loggerFactory);
    }

    /**
     * Creates logger for given class with system logger as a backend.
     *
     * <p>Note: this method should be used to create a server-side logger since user might provide a custom backend for
     * every particular client instance.
     *
     * @param name The name for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forName(String name) {
        var delegate = System.getLogger(name);

        return new IgniteLogger(delegate);
    }

    /**
     * Creates logger for given class with backend created by the given loggerFactory.
     *
     * @param name The name for a logger.
     * @param loggerFactory Logger factory to create backend for the logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forName(String name, LoggerFactory loggerFactory) {
        var delegate = Objects.requireNonNull(loggerFactory, "loggerFactory").forName(name);

        return new IgniteLogger(delegate);
    }

    /**
     * Creates the logger which outputs nothing.
     *
     * @return Void logger.
     */
    public static IgniteLogger voidLogger() {
        return VoidLogger.INSTANCE;
    }
}
