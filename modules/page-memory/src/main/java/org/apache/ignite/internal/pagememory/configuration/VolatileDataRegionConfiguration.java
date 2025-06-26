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

/** * Configuration for a volatile data region. */
public class VolatileDataRegionConfiguration implements DataRegionConfiguration {
    private final String name;
    private final int pageSize;
    private final long initSize;
    private final long maxSize;

    private VolatileDataRegionConfiguration(String name, int pageSize, long initSize, long maxSize) {
        this.name = name;
        this.pageSize = pageSize;
        this.initSize = initSize;
        this.maxSize = maxSize;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    /** Initial memory region size in bytes. */
    public long initSizeBytes() {
        return initSize;
    }

    /** Maximum memory region size in bytes. */
    public long maxSizeBytes() {
        return maxSize;
    }

    /** Creates a builder for {@link VolatileDataRegionConfiguration} instance. */
    public static VolatileDataRegionConfigurationBuilder builder() {
        return new VolatileDataRegionConfigurationBuilder();
    }

    /** Builder for {@link VolatileDataRegionConfiguration}. */
    public static class VolatileDataRegionConfigurationBuilder {
        private String name;
        private int pageSize;
        private long initSize;
        private long maxSize;

        public VolatileDataRegionConfigurationBuilder name(String name) {
            this.name = name;
            return this;
        }

        public VolatileDataRegionConfigurationBuilder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public VolatileDataRegionConfigurationBuilder initSize(long initSize) {
            this.initSize = initSize;
            return this;
        }

        public VolatileDataRegionConfigurationBuilder maxSize(long maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public VolatileDataRegionConfiguration build() {
            return new VolatileDataRegionConfiguration(name, pageSize, initSize, maxSize);
        }
    }
}
