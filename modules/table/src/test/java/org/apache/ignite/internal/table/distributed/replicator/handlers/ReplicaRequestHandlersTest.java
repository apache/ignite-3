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

package org.apache.ignite.internal.table.distributed.replicator.handlers;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ReplicaRequestHandlers}.
 */
class ReplicaRequestHandlersTest {
    private static final short GROUP_A = 1;

    private static final short GROUP_B = 2;

    private static final short TYPE_1 = 10;

    private static final short TYPE_2 = 20;

    @Test
    void handlerIsFoundByGroupAndType() {
        ReplicaRequestHandler<?> handler = (request, replicaPrimacy) -> nullCompletedFuture();

        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addHandler(GROUP_A, TYPE_1, handler);
        ReplicaRequestHandlers handlers = builder.build();

        assertThat(handlers.handler(GROUP_A, TYPE_1), is(notNullValue()));
    }

    @Test
    void handlerReturnsNullForUnregisteredType() {
        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addHandler(GROUP_A, TYPE_1, (request, replicaPrimacy) -> nullCompletedFuture());
        ReplicaRequestHandlers handlers = builder.build();

        assertThat(handlers.handler(GROUP_A, TYPE_2), is(nullValue()));
        assertThat(handlers.handler(GROUP_B, TYPE_1), is(nullValue()));
    }

    @Test
    void roHandlerIsFoundByGroupAndType() {
        ReadOnlyReplicaRequestHandler<?> handler = (request, opStartTs) -> nullCompletedFuture();

        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addRoHandler(GROUP_A, TYPE_1, handler);
        ReplicaRequestHandlers handlers = builder.build();

        assertThat(handlers.roHandler(GROUP_A, TYPE_1), is(notNullValue()));
    }

    @Test
    void roHandlerReturnsNullForUnregisteredType() {
        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addRoHandler(GROUP_A, TYPE_1, (request, opStartTs) -> nullCompletedFuture());
        ReplicaRequestHandlers handlers = builder.build();

        assertThat(handlers.roHandler(GROUP_A, TYPE_2), is(nullValue()));
        assertThat(handlers.roHandler(GROUP_B, TYPE_1), is(nullValue()));
    }

    @Test
    void handlerAndRoHandlerRegistriesAreIndependent() {
        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addHandler(GROUP_A, TYPE_1, (request, replicaPrimacy) -> nullCompletedFuture());
        builder.addRoHandler(GROUP_A, TYPE_2, (request, opStartTs) -> nullCompletedFuture());
        ReplicaRequestHandlers handlers = builder.build();

        assertThat(handlers.handler(GROUP_A, TYPE_1), is(notNullValue()));
        assertThat(handlers.roHandler(GROUP_A, TYPE_1), is(nullValue()));

        assertThat(handlers.roHandler(GROUP_A, TYPE_2), is(notNullValue()));
        assertThat(handlers.handler(GROUP_A, TYPE_2), is(nullValue()));
    }

    @Test
    void duplicateHandlerRegistrationThrows() {
        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addHandler(GROUP_A, TYPE_1, (request, replicaPrimacy) -> nullCompletedFuture());

        assertThrows(IllegalArgumentException.class,
                () -> builder.addHandler(GROUP_A, TYPE_1, (request, replicaPrimacy) -> nullCompletedFuture()));
    }

    @Test
    void duplicateRoHandlerRegistrationThrows() {
        ReplicaRequestHandlers.Builder builder = new ReplicaRequestHandlers.Builder();
        builder.addRoHandler(GROUP_A, TYPE_1, (request, opStartTs) -> nullCompletedFuture());

        assertThrows(IllegalArgumentException.class,
                () -> builder.addRoHandler(GROUP_A, TYPE_1, (request, opStartTs) -> nullCompletedFuture()));
    }
}
