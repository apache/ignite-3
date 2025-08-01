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
import okhttp3.OkHttpClient.Builder;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.jetbrains.annotations.Nullable;

/** Helper class for logging HTTP requests/responses from generated REST API client. */
class HttpLogging {
    private final ApiClient client;

    @Nullable
    private HttpLoggingInterceptor interceptor;

    HttpLogging(ApiClient client) {
        this.client = client;
    }

    /**
     * Starts logging HTTP requests/responses to specified {@code PrintWriter}.
     *
     * @param output Print writer to print logs to.
     * @param verbose Boolean array. Should be non-empty. Number of elements represent verbosity level.
     */
    void startHttpLogging(PrintWriter output, boolean[] verbose) {
        if (interceptor == null) {
            Builder builder = client.getHttpClient().newBuilder();

            interceptor = new HttpLoggingInterceptor(output::println);
            interceptor.setLevel(verbosityLevel(verbose));
            builder.interceptors().add(interceptor);

            client.setHttpClient(builder.build());
        }
    }

    private static Level verbosityLevel(boolean[] verbose) {
        if (verbose.length > 2) {
            return Level.BODY;
        } else if (verbose.length > 1) {
            return Level.HEADERS;
        }
        return Level.BASIC;
    }

    /**
     * Stops logging previously started by {@link HttpLogging#startHttpLogging(PrintWriter, boolean[])}.
     */
    void stopHttpLogging() {
        if (interceptor != null) {
            Builder builder = client.getHttpClient().newBuilder();

            builder.interceptors().remove(interceptor);

            client.setHttpClient(builder.build());

            interceptor = null;
        }
    }
}
