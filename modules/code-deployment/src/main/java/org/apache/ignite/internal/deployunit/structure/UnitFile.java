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

package org.apache.ignite.internal.deployunit.structure;

import java.util.Objects;

/**
 * Represents a file entry in a deployment unit.
 *
 * <p>This implementation of {@link UnitEntry} holds metadata about a single file,
 * including its name and size in bytes.
 */
public class UnitFile implements UnitEntry {
    /** The name of the file. */
    private final String name;

    /** The size of the file in bytes. */
    private final long size;

    /**
     * Creates a new file entry.
     *
     * @param name the file name
     * @param size the file size in bytes
     */
    public UnitFile(String name, long size) {
        this.name = name;
        this.size = size;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitFile unitFile = (UnitFile) o;
        return size == unitFile.size && Objects.equals(name, unitFile.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, size);
    }
}
