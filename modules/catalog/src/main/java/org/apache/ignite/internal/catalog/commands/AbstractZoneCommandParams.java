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
 * Abstract distribution zone ddl command.
 */
public class AbstractZoneCommandParams implements DdlCommandParams {
    /** Default number of zone replicas. */
    public static final int DEFAULT_REPLICA_COUNT = 1;
    /** Default number of zone partitions. */
    public static final int DEFAULT_PARTITION_COUNT = 25;
    /** Default infinite value for the distribution zones' timers. */
    public static final int INFINITE_TIMER_VALUE = Integer.MAX_VALUE;
    /**
     * Default filter value for a distribution zone,
     * which is a {@link com.jayway.jsonpath.JsonPath} expression for including all attributes of nodes.
     */
    public static final String DEFAULT_FILTER = "$..*";

    /** Distribution zone name. */
    protected String zoneName;

    /**
     * Returns distribution zone name.
     */
    public String zoneName() {
        return zoneName;
    }

    /**
     * Parameters builder.
     */
    protected abstract static class AbstractBuilder<ParamT extends AbstractZoneCommandParams, BuilderT> {
        protected ParamT params;

        AbstractBuilder(ParamT params) {
            this.params = params;
        }

        /**
         * Sets distribution zone name.
         *
         * @param zoneName Distribution zone name.
         * @return {@code this}.
         */
        public BuilderT zoneName(String zoneName) {
            params.zoneName = zoneName;

            return (BuilderT) this;
        }

        /**
         * Builds parameters.
         *
         * @return Parameters.
         */
        public ParamT build() {
            ParamT params0 = params;
            params = null;
            return params0;
        }
    }
}
