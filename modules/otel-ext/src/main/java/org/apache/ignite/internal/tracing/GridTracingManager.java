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

package org.apache.ignite.internal.tracing;

import static java.lang.Double.compare;
import static org.apache.ignite.internal.tracing.otel.DynamicRatioSampler.SAMPLING_RATE_NEVER;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.apache.ignite.internal.tracing.configuration.TracingConfiguration;
import org.apache.ignite.internal.tracing.configuration.TracingView;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing Manager.
 */
public class GridTracingManager {
    /**
     * Initialize tracing module.
     *
     * @param name Ignite node name.
     * @param tracingConfiguration Tracing configuration.
     */
    public static void initialize(String name, TracingConfiguration tracingConfiguration) {
        tracingConfiguration.listen((ctx) -> {
            @Nullable TracingView view = ctx.newValue();

            if (view != null && compare(view.ratio(), SAMPLING_RATE_NEVER) != 0) {
                SpanManager spanManager = ServiceLoader
                        .load(SpanManager.class)
                        .stream()
                        .map(Provider::get)
                        .findFirst()
                        .orElse(NoopSpanManager.INSTANCE);

                if (spanManager instanceof Tracing) {
                    Tracing tracing = (Tracing) spanManager;

                    tracing.refreshTracers(name, view);
                }

                TracingManager.initialize(spanManager);
            }

            return nullCompletedFuture();
        });
    }
}
