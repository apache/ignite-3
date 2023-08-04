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

/**
 * Define a data storage.
 */
// TODO: IGNITE-19719 Must be storage engine specific
// TODO: IGNITE-19719 Implement validation of engines and their parameters
public class DataStorageParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    private String engine;

    private String dataRegion;

    /**
     * Returns the storage engine name.
     */
    public String engine() {
        return engine;
    }

    /**
     * Returns the data region name within the storage engine.
     */
    public String dataRegion() {
        return dataRegion;
    }

    /** Parameters builder. */
    public static class Builder {
        private DataStorageParams params;

        private Builder() {
            params = new DataStorageParams();
        }

        /**
         * Sets the storage engine name.
         *
         * @param engine Storage engine name.
         * @return {@code this}.
         */
        public Builder engine(String engine) {
            params.engine = engine;

            return this;
        }

        /**
         * Sets the data region name within the storage engine.
         *
         * @param dataRegion Data region name within the storage engine.
         * @return {@code this}.
         */
        public Builder dataRegion(String dataRegion) {
            params.dataRegion = dataRegion;

            return this;
        }

        /** Builds parameters. */
        public DataStorageParams build() {
            DataStorageParams params0 = params;

            params = null;

            return params0;
        }
    }
}
