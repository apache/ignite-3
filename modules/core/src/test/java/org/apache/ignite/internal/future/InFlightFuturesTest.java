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

package org.apache.ignite.internal.future;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class InFlightFuturesTest {
    private final InFlightFutures inFlightFutures = new InFlightFutures();

    @Test
    void addsFutureToInFlightSetOnRegistration() {
        CompletableFuture<Object> incompleteFuture = new CompletableFuture<>();

        inFlightFutures.registerFuture(incompleteFuture);

        assertThat(currentFutures(inFlightFutures), is(singleton(incompleteFuture)));
    }


    @Test
    void removesAlreadyCompletedFutureOnRegistration() {
        CompletableFuture<Object> completedFuture = CompletableFuture.completedFuture("Completed");

        inFlightFutures.registerFuture(completedFuture);

        assertThat(inFlightFutures, is(emptyIterable()));
    }

    private Set<CompletableFuture<?>> currentFutures(InFlightFutures inFlightFutures) {
        return StreamSupport.stream(inFlightFutures.spliterator(), false).collect(toSet());
    }

    @Test
    void removesFutureFromInFlightSetOnSuccessfulCompletion() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        inFlightFutures.registerFuture(future);

        future.complete("ok");

        assertThat(inFlightFutures, is(emptyIterable()));
    }

    @Test
    void removesFutureFromInFlightSetOnExceptionalCompletion() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        inFlightFutures.registerFuture(future);

        future.completeExceptionally(new Exception());

        assertThat(inFlightFutures, is(emptyIterable()));
    }
}
