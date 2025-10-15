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

import static java.util.Comparator.comparing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.properties.IgniteProductVersion;

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
    private final Map<String, Version> versions = new LinkedHashMap<>();

    public IgniteVersions() {
    }

    /**
     * Constructor.
     *
     * @param artifacts List of dependency notations.
     * @param configOverrides Map of global node configuration overrides.
     * @param storageProfilesOverrides Map of global storage profiles overrides.
     * @param versions List of version descriptors.
     */
    @JsonCreator
    public IgniteVersions(
            @JsonProperty("artifacts") List<String> artifacts,
            @JsonProperty("configOverrides") Map<String, String> configOverrides,
            @JsonProperty("storageProfilesOverrides") Map<String, String> storageProfilesOverrides,
            @JsonProperty("versions") List<Version> versions
    ) {
        this.artifacts = artifacts;
        this.configOverrides = configOverrides;
        this.storageProfilesOverrides = storageProfilesOverrides;

        // Add versions to the linked hash map after sorting by the version
        versions.stream()
                .sorted(comparing(version -> IgniteProductVersion.fromString(version.version())))
                .forEach(version -> this.versions.put(version.version(), version));
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

    public Map<String, Version> versions() {
        return versions;
    }

    /**
     * Gets a value from a version data using specified accessor if the version exists, otherwise returns default value provided by the
     * supplier using this instance.
     *
     * @param version Version to get data for.
     * @param accessor Function to get the value from a version data.
     * @param defaultSupplier Default value function.
     * @param <T> Value type.
     * @return Retrieved value.
     */
    public <T> T getOrDefault(String version, Function<Version, T> accessor, Function<IgniteVersions, T> defaultSupplier) {
        Version versionData = versions.get(version);
        T result = versionData != null ? accessor.apply(versionData) : null;
        return result != null ? result : defaultSupplier.apply(this);
    }

    /**
     * Represents a particular Ignite version with optional node config overrides.
     */
    public static class Version {
        private String version;
        private Map<String, String> configOverrides;
        private Map<String, String> storageProfilesOverrides;

        public Version() {
        }

        /**
         * Constructor.
         *
         * @param version List of dependency notations.
         * @param configOverrides Map of node configuration overrides.
         * @param storageProfilesOverrides Map of node storage profiles overrides.
         */
        @JsonCreator
        public Version(
                @JsonProperty("version") String version,
                @JsonProperty("configOverrides") Map<String, String> configOverrides,
                @JsonProperty("storageProfilesOverrides") Map<String, String> storageProfilesOverrides
        ) {
            this.version = version;
            this.configOverrides = configOverrides;
            this.storageProfilesOverrides = storageProfilesOverrides;
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
