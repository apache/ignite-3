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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.MappedCollection;

/**
 * A class to be a part of nested entities. Root -> Intermediate -> Leaf.
 */
public class Intermediate implements Persistable<Long> {

    @Id
    private Long id;
    private String name;
    private Leaf leaf;
    @MappedCollection(idColumn = "INTERMEDIATE_ID", keyColumn = "INTERMEDIATE_KEY") private List<Leaf> leaves;

    public Intermediate() {}

    /**
     * Constructor.
     *
     * @param id    Id.
     * @param name  Name.
     * @param leaf  Leaf.
     * @param leaves list of leaves.
     */
    public Intermediate(Long id, String name, Leaf leaf, List<Leaf> leaves) {
        this.id = id;
        this.name = name;
        this.leaf = leaf;
        this.leaves = leaves;
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

    public Leaf getLeaf() {
        return this.leaf;
    }

    public List<Leaf> getLeaves() {
        return this.leaves;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Intermediate that = (Intermediate) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name) && Objects.equals(leaf, that.leaf)
                && Objects.equals(leaves, that.leaves);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, leaf, leaves);
    }

    private static long idCounter = 1;

    public static Intermediate createWithLeaf(String namePrefix) {
        return new Intermediate(idCounter++, namePrefix + "Intermediate", Leaf.create(namePrefix), emptyList());
    }

    public static Intermediate createWithLeaves(String namePrefix) {
        return new Intermediate(idCounter++, namePrefix + "Intermediate", null, singletonList(Leaf.create(namePrefix)));
    }
}
