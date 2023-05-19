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

package org.apache.ignite.internal.catalog.commands;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * DROP ZONE statement.
 */
public class CreateZoneParams extends AbstractZoneCommandParams {
    private static final int DEFAULT_PARTITIONS = 25;
    private static final int DEFAULT_REPLICAS = 1;

    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Amount of zone partitions. */
    private int partitions;

    /** Amount of zone partition replicas. */
    private int replicas;

    /**
     * Returns amount of zone partitions.
     */
    public int partitions() {
        return partitions;
    }

    /**
     * Return amount of zone replicas.
     */
    public int replicas() {
        return replicas;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<CreateZoneParams, Builder> {
        Builder() {
            super(new CreateZoneParams());
        }

        /**
         * Sets amount of zone partitions.
         *
         * @param partitions Amount of partitions.
         */
        public Builder partitions(@Nullable Integer partitions) {
            params.partitions = Objects.requireNonNullElse(partitions, DEFAULT_PARTITIONS);

            return this;
        }

        /**
         * Sets amount of zone replicas.
         *
         * @param replicas Amount of replicas.
         */
        public Builder replicas(@Nullable Integer replicas) {
            params.replicas = Objects.requireNonNullElse(replicas, DEFAULT_REPLICAS);

            return this;
        }
    }
}
