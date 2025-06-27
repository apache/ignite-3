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

import static okhttp3.logging.HttpLoggingInterceptor.Level.BASIC;
import static okhttp3.logging.HttpLoggingInterceptor.Level.BODY;
import static okhttp3.logging.HttpLoggingInterceptor.Level.HEADERS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.PrintWriter;
import java.util.List;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CliLoggersTest {
    private final ApiClient client = new ApiClient();

    @AfterEach
    void stopLogging() {
        CliLoggers.stopOutputRedirect();
        CliLoggers.clearLoggers();
    }

    @Test
    void addClient() {
        CliLoggers.addApiClient(client);

        assertThat(CliLoggers.isVerbose(), is(false));

        assertThat(findLoggingInterceptor(), is(nullValue()));
    }

    private static List<Arguments> levels() {
        return List.of(
                arguments(new boolean[]{true}, BASIC),
                arguments(new boolean[]{true, true}, HEADERS),
                arguments(new boolean[]{true, true, true}, BODY),
                arguments(new boolean[]{true, true, true, true}, BODY)
        );
    }

    @ParameterizedTest
    @MethodSource("levels")
    void addClientStartLog(boolean[] verbose, Level level) {
        CliLoggers.addApiClient(client);
        CliLoggers.startOutputRedirect(new PrintWriter(System.out), verbose);

        assertThat(CliLoggers.isVerbose(), is(true));

        HttpLoggingInterceptor loggingInterceptor = findLoggingInterceptor();
        assertThat(loggingInterceptor, is(notNullValue()));
        assertThat(loggingInterceptor.getLevel(), is(level));
    }

    @ParameterizedTest
    @MethodSource("levels")
    void startLogAddClient(boolean[] verbose, Level level) {
        CliLoggers.startOutputRedirect(new PrintWriter(System.out), verbose);
        CliLoggers.addApiClient(client);

        assertThat(CliLoggers.isVerbose(), is(true));

        HttpLoggingInterceptor loggingInterceptor = findLoggingInterceptor();
        assertThat(loggingInterceptor, is(notNullValue()));
        assertThat(loggingInterceptor.getLevel(), is(level));
    }

    @Nullable
    private HttpLoggingInterceptor findLoggingInterceptor() {
        return client.getHttpClient().interceptors().stream()
                .filter(HttpLoggingInterceptor.class::isInstance)
                .map(HttpLoggingInterceptor.class::cast)
                .findFirst()
                .orElse(null);
    }
}
