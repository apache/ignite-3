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

package org.apache.ignite.internal.tostring;

import static org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy.HASH;
import static org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy.NONE;
import static org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy.PLAIN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for output of {@code toString()} depending on the value of {@link SensitiveDataLoggingPolicy}.
 */
public class SensitiveDataToStringTest extends IgniteAbstractTest {
    /** Random int. */
    private static final int rndInt0 = 54321;

    /** Random string. */
    private static final String rndString = "qwer";

    @AfterEach
    @BeforeEach
    public void reset() {
        S.setSensitiveDataPolicy(HASH);
    }

    @Test
    public void testSensitivePropertiesResolving0() {
        assertSame(HASH, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    @Test
    public void testSensitivePropertiesResolving1() {
        S.setSensitiveDataPolicy(PLAIN);
        assertSame(PLAIN, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    @Test
    public void testSensitivePropertiesResolving2() {
        S.setSensitiveDataPolicy(HASH);
        assertSame(HASH, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    @Test
    public void testSensitivePropertiesResolving3() {
        S.setSensitiveDataPolicy(NONE);
        assertSame(NONE, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    @Test
    public void testTableObjectImplWithSensitive() {
        S.setSensitiveDataPolicy(PLAIN);
        testTableObjectImpl((strToCheck, object) -> assertTrue(strToCheck.contains(object.toString()), strToCheck));
    }

    @Test
    public void testTableObjectImplWithHashSensitive() {
        S.setSensitiveDataPolicy(HASH);
        testTableObjectImpl((strToCheck, object) -> assertTrue(strToCheck.contains(object.toString()), strToCheck));
    }

    @Test
    public void testTableObjectImplWithoutSensitive() {
        S.setSensitiveDataPolicy(NONE);
        testTableObjectImpl((strToCheck, object) -> assertEquals("TableObject", object.toString(), strToCheck));
    }

    private void testTableObjectImpl(BiConsumer<String, Object> checker) {
        Person person = new Person(rndInt0, rndString);

        TableObject testObject = new TableObject(person);
        checker.accept(testObject.toString(), testObject);
    }

    static class TableObject {
        @IgniteToStringInclude(sensitive = true)
        Person person;

        TableObject(Person person) {
            this.person = person;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            switch (S.getSensitiveDataLogging()) {
                case PLAIN:
                    return S.toString(getClass().getSimpleName(), "person", person, false);

                case HASH:
                    return String.valueOf(person == null ? "null" : IgniteUtils.hash(person));

                case NONE:
                default:
                    return "TableObject";
            }
        }
    }

    static class Person {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name  Person name.
         */
        Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Person person = (Person) o;
            return orgId == person.orgId && (name != null ? name.equals(person.name) : person.name == null);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(orgId, name);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
