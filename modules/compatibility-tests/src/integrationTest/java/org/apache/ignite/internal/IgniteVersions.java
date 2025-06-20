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
    public static IgniteVersions INSTANCE = readFromJson();

    private List<String> artifacts;
    private List<Version> versions;

    public IgniteVersions() {
    }

    @JsonCreator
    public IgniteVersions(@JsonProperty("artifacts") List<String> artifacts, @JsonProperty("versions") List<Version> versions) {
        this.artifacts = artifacts;
        this.versions = versions;
    }

    public List<String> artifacts() {
        return artifacts;
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

        public Version() {
        }

        @JsonCreator
        public Version(@JsonProperty("version") String version, @JsonProperty("configOverrides") Map<String, String> configOverrides) {
            this.version = version;
            this.configOverrides = configOverrides;
        }

        public String version() {
            return version;
        }

        public Map<String, String> configOverrides() {
            return configOverrides;
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
