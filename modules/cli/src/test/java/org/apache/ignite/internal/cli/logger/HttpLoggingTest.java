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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

import java.io.PrintWriter;
import java.io.StringWriter;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient.Builder;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpLoggingTest {

    private static final PrintWriter WRITER = new PrintWriter(new StringWriter());

    private ApiClient client;

    private HttpLogging logger;

    @BeforeEach
    void setUp() {
        client = new ApiClient();
        logger = new HttpLogging(client);
    }

    @Test
    void startAndStopLogging() {
        assertThat(client.getHttpClient().interceptors(), empty());

        logger.startHttpLogging(WRITER);
        assertThat(client.getHttpClient().interceptors(), not(empty()));

        logger.stopHttpLogging();
        assertThat(client.getHttpClient().interceptors(), empty());
    }

    @Test
    void restartLogging() {
        assertThat(client.getHttpClient().interceptors(), empty());

        logger.startHttpLogging(WRITER);
        assertThat(client.getHttpClient().interceptors(), not(empty()));

        logger.stopHttpLogging();
        assertThat(client.getHttpClient().interceptors(), empty());

        logger.startHttpLogging(WRITER);
        assertThat(client.getHttpClient().interceptors(), not(empty()));
    }

    @Test
    void stopLoggingRemoveOnlyOneInterceptor() {
        Interceptor interceptor = chain -> chain.proceed(chain.request());
        Builder builder = client.getHttpClient().newBuilder();
        builder.interceptors().add(interceptor);
        client.setHttpClient(builder.build());

        logger.startHttpLogging(WRITER);
        logger.stopHttpLogging();

        assertThat(client.getHttpClient().interceptors(), contains(interceptor));
    }
}
