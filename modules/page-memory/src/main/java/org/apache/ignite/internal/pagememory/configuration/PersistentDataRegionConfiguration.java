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

package org.apache.ignite.internal.pagememory.configuration;

import org.apache.ignite.internal.pagememory.persistence.throttling.ThrottlingPolicyFactory;

/** * Configuration for a persistent data region. */
public class PersistentDataRegionConfiguration implements DataRegionConfiguration {
    private final String name;
    private final int pageSize;
    private final long size;
    private final ReplacementMode replacementMode;
    private final ThrottlingPolicyFactory throttlingPolicyFactory;

    @Override
    public String name() {
        return name;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    public long sizeBytes() {
        return size;
    }

    /** Replacement mode for evicted pages. */
    public ReplacementMode replacementMode() {
        return replacementMode;
    }

    public ThrottlingPolicyFactory throttlingPolicyFactory() {
        return throttlingPolicyFactory;
    }

    private PersistentDataRegionConfiguration(
            String name, int pageSize, long size, ReplacementMode replacementMode, ThrottlingPolicyFactory throttlingPolicyFactory
    ) {
        this.name = name;
        this.pageSize = pageSize;
        this.size = size;
        this.replacementMode = replacementMode;
        this.throttlingPolicyFactory = throttlingPolicyFactory;
    }

    /** Creates a builder for {@link PersistentDataRegionConfiguration} instance. */
    public static PersistentDataRegionConfigurationBuilder builder() {
        return new PersistentDataRegionConfigurationBuilder();
    }

    /** Builder for {@link PersistentDataRegionConfiguration}. */
    public static class PersistentDataRegionConfigurationBuilder {
        private String name;
        private int pageSize;
        private long size;
        private ReplacementMode replacementMode = ReplacementMode.CLOCK;
        private ThrottlingPolicyFactory throttlingPolicyFactory = pageMemory -> null;

        public PersistentDataRegionConfigurationBuilder name(String name) {
            this.name = name;
            return this;
        }

        public PersistentDataRegionConfigurationBuilder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public PersistentDataRegionConfigurationBuilder size(long size) {
            this.size = size;
            return this;
        }

        public PersistentDataRegionConfigurationBuilder replacementMode(ReplacementMode replacementMode) {
            this.replacementMode = replacementMode;
            return this;
        }

        public PersistentDataRegionConfigurationBuilder throttlingPolicyFactory(ThrottlingPolicyFactory throttlingPolicyFactory) {
            this.throttlingPolicyFactory = throttlingPolicyFactory;
            return this;
        }

        public PersistentDataRegionConfiguration build() {
            return new PersistentDataRegionConfiguration(name, pageSize, size, replacementMode, throttlingPolicyFactory);
        }
    }
}
