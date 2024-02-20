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

package org.apache.ignite.example;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.tracing.TracingManager.wrap;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.tracing.TraceSpan;

/**
 * Tests for propagating context between threads.
 */
public class ThreadExample {
    private static CompletableFuture<Integer> process(Integer delay) {
        try (var ignored = span("process")) {
            try {
                SECONDS.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }

            return completedFuture(10);
        }
    }

    private static void complete(CompletableFuture<Integer> f) {
        try (var ignored = span("complete")) {
            f.complete(1);
        }
    }

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        TraceSpan parent;

        try (var ignored = rootSpan("ClientTransactionBeginRequest.process")) {
            try (var parent0 = asyncSpan("main1")) {
                parent = parent0;
            }
        }

        span(parent, "ClientTupleGetRequest.process", (Function<TraceSpan, CompletableFuture<Integer>>) (span) ->
            completedFuture(10)).join();



        span(parent, "main", (rootSpan) -> {
            System.out.println(rootSpan);
        });

        parent.end();

        try (var rootSpan = rootSpan("main")) {
            try (var ignored = span("main1")) {
                System.out.println(ignored);
            }

            var f = new CompletableFuture<Integer>();

            new Thread(() -> f.thenCompose(ThreadExample::process)).start();
            new Thread(wrap(() -> complete(f))).start();
        }
    }
}
