/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.serialization;

/**
 * Describes how a class is to be serialized.
 */
public class Serialization {
    /** Serialization type (see {@link SerializationType}. */
    private final int type;

    /** Combination of flags defined in Feature. */
    private final int features;

    public Serialization(int type, int features) {
        this.type = type;
        this.features = features;
    }

    public Serialization(int type) {
        this(type, 0);
    }

    public int type() {
        return type;
    }

    public int features() {
        return features;
    }

    public boolean hasOverride() {
        return (features & Feature.OVERRIDE) != 0;
    }

    public boolean hasWriteReplace() {
        return (features & Feature.WRITE_REPLACE) != 0;
    }

    public boolean hasReadResolve() {
        return (features & Feature.READ_RESOLVE) != 0;
    }

    /**
     * Serialization-related features.
     */
    public static class Feature {
        public static final int OVERRIDE = 1;
        public static final int WRITE_REPLACE = 2;
        public static final int READ_RESOLVE = 4;
    }
}
