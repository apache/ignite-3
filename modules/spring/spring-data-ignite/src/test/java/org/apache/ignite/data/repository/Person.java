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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.jdbc.core.mapping.AggregateReference;

/**
 * Test class.
 */
public class Person implements Persistable<Long> {
    @Id
    private Long id;

    private String name;

    private Boolean flag;

    private Direction direction;

    @Transient
    private boolean isNew = true;

    AggregateReference<Person, Long> ref;

    public Person() {}

    /**
     * Constructor.
     *
     * @param id Id.
     * @param name Name.
     */
    public Person(Long id, String name) {
        this.id = id;
        this.name = name;
        this.direction = Direction.LEFT;
        this.flag = false;
        this.ref = () -> 0L;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean newValue) {
        isNew = newValue;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getFlag() {
        return flag;
    }

    public void setFlag(Boolean flag) {
        this.flag = flag;
    }

    public AggregateReference<Person, Long> getRef() {
        return ref;
    }

    public void setRef(AggregateReference<Person, Long> ref) {
        this.ref = ref;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    /**
     * Enum for testing purposes. Doesn't have semantic meaning.
     */
    public enum Direction {
        LEFT, CENTER, RIGHT
    }

    private static long idCounter = 1;

    /**
     * Constructor.
     * Id will be generated.
     */
    public static Person create() {
        return new Person(idCounter++, "Name");
    }

    /**
     * Constructor.
     * Id will be generated.
     *
     * @param name Name.
     */
    public static Person create(String name) {
        Person person = new Person(idCounter++, name);
        person.setName(name);
        return person;
    }

    /**
     * Constructor.
     *
     * @param id Id.
     * @param name Name.
     */
    public static Person create(Long id, String name) {
        Person person = new Person(id, name);
        person.setName(name);
        return person;
    }
}
