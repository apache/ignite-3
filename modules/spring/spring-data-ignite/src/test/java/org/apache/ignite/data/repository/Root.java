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

package org.apache.ignite.data.repository;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.MappedCollection;

/**
 * A class to be a part of nested entities. Root -> Intermediate -> Leaf.
 */
public class Root implements Persistable<Long> {

    @Id
    private  Long id;
    private String name;
    private Intermediate intermediate;
    @MappedCollection(idColumn = "ROOT_ID", keyColumn = "ROOT_KEY") private  List<Intermediate> intermediates;

    public Root() {}

    private Root(Long id, String name, Intermediate intermediate, List<Intermediate> intermediates) {
        this.id = id;
        this.name = name;
        this.intermediate = intermediate;
        this.intermediates = intermediates;
    }

    @Override
    public Long getId() {
        return this.id;
    }

    @Override
    public boolean isNew() {
        return true;
    }

    public String getName() {
        return this.name;
    }

    public Intermediate getIntermediate() {
        return this.intermediate;
    }

    public List<Intermediate> getIntermediates() {
        return this.intermediates;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Root root = (Root) o;
        return Objects.equals(id, root.id) && Objects.equals(name, root.name) && Objects.equals(intermediate,
                root.intermediate) && Objects.equals(intermediates, root.intermediates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, intermediate, intermediates);
    }

    private static long idCounter = 1;

    /**
     * Creates {@link Root}.
     *
     * @param namePrefix Name prefix.
     */
    public static Root create(String namePrefix) {
        return new Root(idCounter++, namePrefix,
                Intermediate.createWithLeaf(namePrefix),
                singletonList(Intermediate.createWithLeaves(namePrefix)));
    }
}
