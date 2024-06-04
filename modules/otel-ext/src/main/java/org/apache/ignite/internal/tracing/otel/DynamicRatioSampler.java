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

package org.apache.ignite.internal.tracing.otel;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;

/**
 * Dynamic ratio sampler.
 */
public class DynamicRatioSampler implements Sampler {
    /** Min valid sampling rate with special meaning that span won't be created. */
    public static final double SAMPLING_RATE_NEVER = 0.0d;

    /** Max valid sampling rate with special meaning that span will be always created. */
    private static final double SAMPLING_RATE_ALWAYS = 1.0d;

    /** Current sampler configured to make sampling decision. */
    private volatile Sampler sampler;

    public DynamicRatioSampler() {
        this(Sampler.parentBased(Sampler.alwaysOff()));
    }

    private DynamicRatioSampler(Sampler sampler) {
        this.sampler = sampler;
    }

    /** {@inheritDoc} */
    public synchronized void configure(double ratio, boolean parentBased) {
        Sampler s;
        if (ratio == SAMPLING_RATE_NEVER) {
            s = Sampler.alwaysOff();
        } else if (ratio == SAMPLING_RATE_ALWAYS) {
            s = Sampler.alwaysOn();
        } else {
            s = Sampler.traceIdRatioBased(ratio);
        }

        if (parentBased) {
            s = Sampler.parentBased(s);
        }

        sampler = s;
    }

    /** {@inheritDoc} */
    @Override
    public SamplingResult shouldSample(Context parentContext, String traceId, String name, SpanKind spanKind, Attributes attributes,
            List<LinkData> parentLinks) {

        return sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription() {
        return "Dynamic ratio sampler: " + sampler.getDescription();
    }
}
