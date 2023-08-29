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
 * ALTER ZONE RENAME TO statement.
 */
public class RenameZoneParams extends AbstractZoneCommandParams {
    /** Constructor. */
    private RenameZoneParams(String zoneName, String newZoneName) {
        super(zoneName);

        this.newZoneName = newZoneName;
    }

    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** New zone name. */
    private final String newZoneName;

    /**
     * Returns new name for a zone.
     */
    public String newZoneName() {
        return newZoneName;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<RenameZoneParams, Builder> {
        private String newZoneName;

        /**
         * Sets new name for a zone.
         *
         * @param zoneName New zone name.
         */
        public Builder newZoneName(String zoneName) {
            this.newZoneName = zoneName;

            return this;
        }

        @Override
        protected RenameZoneParams createParams() {
            return new RenameZoneParams(zoneName, newZoneName);
        }
    }
}
