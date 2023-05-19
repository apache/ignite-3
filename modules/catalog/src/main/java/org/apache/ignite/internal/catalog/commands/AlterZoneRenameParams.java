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
 * ALTER ZONE RENAME statement.
 */
public class AlterZoneRenameParams extends AbstractZoneCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** New zone name. */
    private String newZoneName;

    /**
     * Returns new name for the zone.
     */
    public String newZoneName() {
        return newZoneName;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<AlterZoneRenameParams, Builder> {
        Builder() {
            super(new AlterZoneRenameParams());
        }

        /**
         * Sets new zone name.
         *
         * @param zoneName New zone name.
         */
        public Builder newZoneName(String zoneName) {
            params.newZoneName = zoneName;

            return this;
        }
    }
}
