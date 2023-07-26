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

package org.apache.igntie.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.ignite.serialization.JsonObjectSerializer;
import org.apache.ignite.serialization.UserObjectSerializer;
import org.junit.jupiter.api.Test;

public class JsonSerializerTest {
    private final UserObjectSerializer serializer = new JsonObjectSerializer();

    @Test
    public void test() {
        Person person = new Person("name", 1);
        byte[] serialize = serializer.serialize(person);
        System.out.println(new String(serialize, StandardCharsets.UTF_8));

        Person person1 = serializer.deserialize(serialize, Person.class);

        assertEquals(person, person1);
    }

    @Test
    public void testWithGenericClass() {
        List<List<String>> list = List.of(List.of("1", "2"), List.of("3"), List.of("4", "5"));
        WithGenericField withGenericField = new WithGenericField("s", list);

        byte[] serialize = serializer.serialize(withGenericField);
        System.out.println(new String(serialize, StandardCharsets.UTF_8));

        WithGenericField generic1 = serializer.deserialize(serialize, WithGenericField.class);

        assertEquals(withGenericField, generic1);
    }

    @Test
    public void testGeneric() {
        GenericClass<Person> generic = new GenericClass<>("s", new Person("name", 1));
        byte[] serialize = serializer.serialize(generic);
        System.out.println(new String(serialize, StandardCharsets.UTF_8));

        GenericClass<Person> generic1 = serializer.deserialize(serialize, GenericClass.class);

        assertEquals(generic, generic1);

        GenericClass<List<List<Person>>> generic2 = new GenericClass<>("s",
                List.of(
                        List.of(new Person("name1", 1), new Person("name2", 2)),
                        List.of(new Person("name3", 3))
                )
        );

        byte[] serialize1 = serializer.serialize(generic2);
        System.out.println(new String(serialize1, StandardCharsets.UTF_8));

        GenericClass<List<List<Person>>> generic3 = serializer.deserialize(serialize1, GenericClass.class);

        assertEquals(generic2, generic3);
    }

    private static class Person {
        public String name;
        public long l;

        public Person(String name, long l) {
            this.name = name;
            this.l = l;
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

            if (l != person.l) {
                return false;
            }
            return name != null ? name.equals(person.name) : person.name == null;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (int) (l ^ (l >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", l=" + l +
                    '}';
        }
    }

    private static class WithGenericField {
        private String s;
        private List<List<String>> list;

        public WithGenericField(String s, List<List<String>> list) {
            this.s = s;
            this.list = list;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WithGenericField that = (WithGenericField) o;

            if (s != null ? !s.equals(that.s) : that.s != null) {
                return false;
            }
            return list != null ? list.equals(that.list) : that.list == null;
        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + (list != null ? list.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "WithGenericField{" +
                    "s='" + s + '\'' +
                    ", list=" + list +
                    '}';
        }
    }

    private static class GenericClass<T> {
        private T type;
        private String s;

        private GenericClass(String s, T type) {
            this.type = type;
            this.s = s;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GenericClass<?> generic = (GenericClass<?>) o;

            if (type != null ? !type.equals(generic.type) : generic.type != null) {
                return false;
            }
            return s != null ? s.equals(generic.s) : generic.s == null;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (s != null ? s.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Generic{" +
                    "type=" + type +
                    ", s='" + s + '\'' +
                    '}';
        }
    }
}
