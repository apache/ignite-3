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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a folder (directory) entry in a deployment unit.
 *
 * <p>This implementation of {@link UnitEntry} holds metadata about a folder,
 * including its name and children entries. The folder can contain both files
 * ({@link UnitFile}) and other folders ({@link UnitFolder}), creating a hierarchical structure.
 *
 * <p>The size of a folder is calculated as the sum of all its children's sizes.
 */
public class UnitFolder implements UnitEntry {
    /** The name of the folder. */
    private final String name;

    /** The list of children entries in this folder. */
    private final List<UnitEntry> children = new ArrayList<>();

    /**
     * Creates a new folder entry with the specified name.
     *
     * @param name the folder name
     */
    public UnitFolder(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long size() {
        return children.stream().mapToLong(UnitEntry::size).sum();
    }

    /**
     * Adds a child entry to this folder.
     *
     * @param entry the child entry to add (file or folder)
     */
    public void addChild(UnitEntry entry) {
        children.add(entry);
    }

    /**
     * Returns an unmodifiable collection of children entries.
     *
     * @return the children entries, never {@code null}
     */
    public Collection<UnitEntry> children() {
        return Collections.unmodifiableCollection(children);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitFolder folder = (UnitFolder) o;
        return Objects.equals(name, folder.name) && Objects.equals(children, folder.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, children);
    }
}
