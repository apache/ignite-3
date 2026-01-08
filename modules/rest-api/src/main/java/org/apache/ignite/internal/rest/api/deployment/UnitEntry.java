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

package org.apache.ignite.internal.rest.api.deployment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.util.List;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry.UnitFile;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry.UnitFolder;

/**
 * Represents an entry in a deployment unit's content structure for REST API responses.
 *
 * <p>A unit entry can be either a {@link UnitFile} or a {@link UnitFolder}.
 * This interface is used for JSON serialization/deserialization in the REST API,
 * with polymorphic type handling based on the "type" property.
 *
 * <p>The JSON representation includes a discriminator field "type" that can be either
 * "file" or "folder" to distinguish between the two implementations.
 */
@Schema(description = "Unit content entry.")
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @Type(value = UnitFile.class, name = "file"),
        @Type(value = UnitFolder.class, name = "folder")
})
public interface UnitEntry {
    /**
     * Returns the name of this entry.
     *
     * @return the entry name
     */
    @JsonGetter("name")
    String name();

    /**
     * Returns the size of this entry in bytes.
     *
     * <p>For files, this is the actual file size. For folders, this is the sum
     * of all children sizes (recursive).
     *
     * @return the entry size in bytes
     */
    @JsonGetter("size")
    long size();

    /**
     * Represents a file entry in a deployment unit for REST API responses.
     *
     * <p>This nested class implements {@link UnitEntry} and is serialized with
     * a "type": "file" discriminator in JSON responses.
     */
    @Schema(description = "Unit content file.")
    public class UnitFile implements UnitEntry {
        @Schema(description = "Unit content file name.", requiredMode = RequiredMode.REQUIRED)
        private final String name;

        @Schema(description = "Unit content file size in bytes.", requiredMode = RequiredMode.REQUIRED)
        private final long size;

        /**
         * Creates a new file entry for JSON deserialization.
         *
         * @param name the file name
         * @param size the file size in bytes
         */
        @JsonCreator
        public UnitFile(
                @JsonProperty("name") String name,
                @JsonProperty("size") long size
        ) {
            this.name = name;
            this.size = size;
        }

        /**
         * Returns the name of this file.
         *
         * @return the file name
         */
        @JsonGetter("name")
        @Override
        public String name() {
            return name;
        }

        /**
         * Returns the size of this file in bytes.
         *
         * @return the file size in bytes
         */
        @JsonGetter("size")
        @Override
        public long size() {
            return size;
        }
    }

    /**
     * Represents a folder (directory) entry in a deployment unit for REST API responses.
     *
     * <p>This nested class implements {@link UnitEntry} and is serialized with
     * a "type": "folder" discriminator in JSON responses. It contains a list of
     * children entries, which can be either files or other folders, creating a
     * hierarchical structure.
     *
     * <p>The size of a folder is calculated as the sum of all its children's sizes.
     */
    @Schema(description = "Unit content folder.")
    public class UnitFolder implements UnitEntry {
        @Schema(description = "Unit content folder name.", requiredMode = RequiredMode.REQUIRED)
        private final String name;

        @Schema(description = "Unit content folder elements.", requiredMode = RequiredMode.REQUIRED)
        private final List<UnitEntry> children;

        /**
         * Creates a new folder entry for JSON deserialization.
         *
         * @param name the folder name
         * @param children the list of children entries
         */
        @JsonCreator
        public UnitFolder(
                @JsonProperty("name") String name,
                @JsonProperty("children") List<UnitEntry> children
        ) {
            this.name = name;
            this.children = children;
        }

        /**
         * Returns the name of this folder.
         *
         * @return the folder name
         */
        @JsonGetter("name")
        @Override
        public String name() {
            return name;
        }

        /**
         * Returns the total size of this folder in bytes.
         *
         * <p>The size is calculated as the sum of all children sizes.
         *
         * @return the total folder size in bytes
         */
        @Override
        public long size() {
            return children.stream().mapToLong(UnitEntry::size).sum();
        }

        /**
         * Returns the list of children entries in this folder.
         *
         * @return the list of children entries
         */
        @JsonGetter("children")
        public List<UnitEntry> children() {
            return children;
        }
    }
}
