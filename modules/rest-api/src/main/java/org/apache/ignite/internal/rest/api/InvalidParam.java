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

package org.apache.ignite.internal.rest.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;

/**
 * Parameter validation result.
 */
@Schema(description = "Information about invalid request parameter.")
public class InvalidParam {
    @Schema(description = "Parameter name.")
    private final String name;
    @Schema(description = "The issue with the parameter.")
    private final String reason;

    @JsonCreator
    public InvalidParam(
            @JsonProperty("name") String name,
            @JsonProperty("reason") String reason
    ) {
        this.name = name;
        this.reason = reason;
    }

    @JsonGetter("name")
    public String name() {
        return name;
    }

    @JsonGetter("reason")
    public String reason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InvalidParam that = (InvalidParam) o;
        return Objects.equals(name, that.name) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, reason);
    }

    @Override
    public String toString() {
        return "InvalidParam{"
                + "name='" + name + '\''
                + ", reason='" + reason + '\''
                + '}';
    }
}
