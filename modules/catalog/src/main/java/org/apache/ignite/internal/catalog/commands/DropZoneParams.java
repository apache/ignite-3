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
 * DROP ZONE statement.
 */
public class DropZoneParams implements DdlCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

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
    public static class Builder {
        DropZoneParams params;

        Builder() {
            params = new DropZoneParams();
        }

        /**
         * Sets distribution zone name.
         *
         * @param zoneName Distribution zone name.
         * @return {@code this}.
         */
        public Builder zoneName(String zoneName) {
            params.zoneName = zoneName;

            return this;
        }

        /**
         * Builds parameters.
         *
         * @return Parameters.
         */
        public DropZoneParams build() {
            DropZoneParams params0 = params;
            params = null;
            return params0;
        }
    }
}
