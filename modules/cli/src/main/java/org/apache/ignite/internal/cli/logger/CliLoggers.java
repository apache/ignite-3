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

package org.apache.ignite.internal.cli.logger;

import java.io.PrintWriter;
import java.lang.System.Logger;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.LoggerFactory;
import org.apache.ignite.rest.client.invoker.ApiClient;

/**
 * This class is used when verbose output for command is needed. Instances of loggers created by the {@link CliLoggers#forClass(Class)}
 * method will redirect their output to the console when commands are started with the {@code -v} flag.
 */
public class CliLoggers {
    private static PrintWriter output;

    private static boolean isVerbose;

    private static boolean[] verbose;

    /** Http loggers for the REST API clients. */
    private static final Map<String, HttpLogging> httpLoggers = new ConcurrentHashMap<>();

    private static final LoggerFactory loggerFactory = name -> new CliLogger(System.getLogger(name));

    /**
     * Creates logger for given class.
     *
     * @param cls The class for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls) {
        return Loggers.forClass(cls, loggerFactory);
    }

    /**
     * Registers a client. If verbose logging is enabled, turn the logging for this client on.
     *
     * @param client Api client.
     */
    public static void addApiClient(ApiClient client) {
        HttpLogging logger = httpLoggers.computeIfAbsent(client.getBasePath(), s -> new HttpLogging(client));
        if (isVerbose) {
            logger.startHttpLogging(output, verbose);
        }
    }

    /**
     * Unregisters clients.
     */
    public static void clearLoggers() {
        httpLoggers.clear();
    }

    /**
     * Starts redirecting output from loggers and from REST API client to the specified print writer.
     *
     * @param out Print writer to write logs to.
     * @param verbose Boolean array. Should be non-empty. Number of elements represent verbosity level.
     */
    public static void startOutputRedirect(PrintWriter out, boolean[] verbose) {
        output = out;
        isVerbose = true;
        CliLoggers.verbose = verbose;
        httpLoggers.values().forEach(logger -> logger.startHttpLogging(out, verbose));
    }

    /**
     * Stops redirecting output previously started by {@link CliLoggers#startOutputRedirect(PrintWriter, boolean[])}.
     */
    public static void stopOutputRedirect() {
        if (output != null) {
            output.flush();
        }
        output = null;
        isVerbose = false;
        verbose = new boolean[0];
        httpLoggers.values().forEach(HttpLogging::stopHttpLogging);
    }

    /**
     * Determine whether the output is redirected or not.
     *
     * @return {@code true} if output is redirected.
     */
    public static boolean isVerbose() {
        return isVerbose;
    }

    /**
     * Returns the current verbose level (0 = not verbose, 1 = -v, 2 = -vv, 3 = -vvv).
     *
     * @return verbosity level.
     */
    public static int getVerboseLevel() {
        return verbose == null ? 0 : verbose.length;
    }

    /**
     * Writes a verbose message if the current verbose level is at least {@code minLevel}.
     *
     * @param minLevel Minimum verbosity level required for this message.
     * @param message Message to print.
     */
    public static void verboseLog(int minLevel, String message) {
        if (isVerbose && verbose.length >= minLevel) {
            output.println(message);
            output.flush();
        }
    }

    private static class CliLogger implements Logger {

        private final Logger delegate;

        private CliLogger(Logger delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public boolean isLoggable(Level level) {
            return delegate.isLoggable(level) || isVerbose;
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
            if (isVerbose) {
                output.println(msg);
                output.println(thrown.getMessage());
            }
            delegate.log(level, bundle, msg, thrown);
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            if (isVerbose) {
                output.println(format);
            }
            delegate.log(level, bundle, format, params);
        }
    }
}
