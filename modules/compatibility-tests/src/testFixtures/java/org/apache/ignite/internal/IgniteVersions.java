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

package org.apache.ignite.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.logger.Loggers;

/**
 * POJO with ignite versions data from the {@code igniteVersions.json}. Contains a list of artifact names and a list of versions with
 * optional node config overrides.
 */
@SuppressWarnings("unused")
public class IgniteVersions {
    public static final IgniteVersions INSTANCE = readFromJson();

    private List<String> artifacts;
    private Map<String, String> configOverrides;
    private Map<String, String> storageProfilesOverrides;
    private List<String> apiExcludes;
    private List<Version> versions;

    public IgniteVersions() {
    }

    /**
     * Constructor.
     *
     * @param artifacts List of dependency notations.
     * @param configOverrides Map of global node configuration overrides.
     * @param storageProfilesOverrides Map of global storage profiles overrides.
     * @param versions List of version descriptors.
     * @param apiExcludes Java API test excludes.
     */
    @JsonCreator
    public IgniteVersions(
            @JsonProperty("artifacts") List<String> artifacts,
            @JsonProperty("configOverrides") Map<String, String> configOverrides,
            @JsonProperty("storageProfilesOverrides") Map<String, String> storageProfilesOverrides,
            @JsonProperty("versions") List<Version> versions,
            @JsonProperty("apiExcludes") List<String> apiExcludes
    ) {
        this.artifacts = artifacts;
        this.configOverrides = configOverrides;
        this.storageProfilesOverrides = storageProfilesOverrides;
        this.versions = versions;
        this.apiExcludes = apiExcludes != null ? apiExcludes : List.of();
    }

    public List<String> artifacts() {
        return artifacts;
    }

    public Map<String, String> configOverrides() {
        return configOverrides;
    }

    public Map<String, String> storageProfilesOverrides() {
        return storageProfilesOverrides;
    }

    public List<String> apiExcludes() {
        return apiExcludes;
    }

    public List<Version> versions() {
        return versions;
    }

    /**
     * Represents a particular Ignite version with optional node config overrides.
     */
    public static class Version {
        private String version;
        private Map<String, String> configOverrides;
        private Map<String, String> storageProfilesOverrides;
        private List<String> apiExcludes;

        public Version() {
        }

        /**
         * Constructor.
         *
         * @param version List of dependency notations.
         * @param configOverrides Map of node configuration overrides.
         * @param storageProfilesOverrides Map of node storage profiles overrides.
         * @param apiExcludes Java API test excludes.
         */
        @JsonCreator
        public Version(
                @JsonProperty("version") String version,
                @JsonProperty("configOverrides") Map<String, String> configOverrides,
                @JsonProperty("storageProfilesOverrides") Map<String, String> storageProfilesOverrides,
                @JsonProperty("apiExcludes") List<String> apiExcludes
        ) {
            this.version = version;
            this.configOverrides = configOverrides;
            this.storageProfilesOverrides = storageProfilesOverrides;
            this.apiExcludes = apiExcludes != null ? apiExcludes : List.of();
        }

        public String version() {
            return version;
        }

        public Map<String, String> configOverrides() {
            return configOverrides;
        }

        public Map<String, String> storageProfilesOverrides() {
            return storageProfilesOverrides;
        }

        public List<String> apiExcludes() {
            return apiExcludes;
        }
    }

    private static IgniteVersions readFromJson() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(IgniteVersions.class.getResource("/igniteVersions.json"), IgniteVersions.class);
        } catch (IOException e) {
            Loggers.forClass(IgniteVersions.class).error("Failed to read igniteVersions.json", e);
            return new IgniteVersions();
        }
    }
}
