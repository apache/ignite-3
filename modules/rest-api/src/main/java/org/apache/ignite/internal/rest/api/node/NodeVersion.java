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

package org.apache.ignite.internal.rest.api.node;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;

/**
 * Node version information that is returned by REST.
 */
@Schema(description = "Node version.")
public class NodeVersion {
    @Schema(description = "Node version.", requiredMode = RequiredMode.REQUIRED)
    private final String version;

    @Schema(description = "Node product.", requiredMode = RequiredMode.REQUIRED)
    private final String product;

    /**
     * Construct NodeVersion DTO.
     */
    private NodeVersion(String version, String product) {
        this.version = version;
        this.product = product;
    }

    @JsonGetter("version")
    public String version() {
        return version;
    }

    @JsonGetter("product")
    public String product() {
        return product;
    }

    /** Constructs new builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder class. */
    public static class Builder {
        private String version;

        private String product;

        /**
         * Sets version.
         *
         * @param version Version.
         * @return This instance.
         */
        public Builder version(String version) {
            this.version = version;
            return this;
        }

        /**
         * Sets product.
         *
         * @param product Product name.
         * @return This instance.
         */
        public Builder product(String product) {
            this.product = product;
            return this;
        }

        /**
         * Builds version object.
         *
         * @return Constructed version object.
         */
        public NodeVersion build() {
            return new NodeVersion(version, product);
        }
    }
}
