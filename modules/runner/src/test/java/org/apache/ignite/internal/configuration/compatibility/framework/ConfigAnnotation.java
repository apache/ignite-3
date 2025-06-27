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

package org.apache.ignite.internal.configuration.compatibility.framework;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class describes annotation.
 */
public class ConfigAnnotation {
    @JsonProperty
    private String name;
    @JsonProperty
    private Map<String, String> properties;

    ConfigAnnotation() {
        // Default constructor for Jackson deserialization.
    }

    ConfigAnnotation(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public boolean equals(Object o) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-25747 Validate annotations properly.
        return o != null && getClass() == o.getClass() && name.equals(((ConfigAnnotation) o).name);

    }

    @Override
    public String toString() {
        return name + (properties == null || properties.isEmpty() ? ""
                : properties.entrySet().stream()
                        .map(Map.Entry::toString)
                        .collect(Collectors.joining(",", "(", ")")));
    }
}
