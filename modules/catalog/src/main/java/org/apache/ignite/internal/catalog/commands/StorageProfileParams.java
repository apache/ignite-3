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
 *  Define a storage profile.
 */
public class StorageProfileParams {
    /** Creates parameters builder. */
    public static StorageProfileParams.Builder builder() {
        return new StorageProfileParams.Builder();
    }

    private String storageProfile;

    /**
     * Returns storage profiles.
     */
    public String storageProfile() {
        return storageProfile;
    }

    /** Parameters builder. */
    public static class Builder {
        private StorageProfileParams params;

        private Builder() {
            params = new StorageProfileParams();
        }

        /**
         * Sets the storage profiles.
         *
         * @param storageProfiles Storage profiles.
         * @return {@code this}.
         */
        public StorageProfileParams.Builder storageProfile(String storageProfiles) {
            params.storageProfile = storageProfiles;

            return this;
        }

        /** Builds parameters. */
        public StorageProfileParams build() {
            StorageProfileParams params0 = params;

            params = null;

            return params0;
        }
    }
}
