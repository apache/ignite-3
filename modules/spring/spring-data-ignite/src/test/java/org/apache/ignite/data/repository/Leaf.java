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

import java.util.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;

/**
 * A class to be a part of nested entities. Root -> Intermediate -> Leaf.
 */
public class Leaf implements Persistable<Long> {

    @Id
    private Long id;
    private String name;

    public Leaf(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Leaf() {

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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Leaf leaf = (Leaf) o;
        return Objects.equals(id, leaf.id) && Objects.equals(name, leaf.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    private static long idCounter = 1;

    public static Leaf create(String namePrefix) {
        return new Leaf(idCounter++, namePrefix + "QualifiedLeaf");
    }
}
