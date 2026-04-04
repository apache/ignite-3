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

package org.apache.ignite.internal.schema.marshaller;

import java.util.Objects;
import org.apache.ignite.catalog.annotations.Column;

/**
 * Declaration variants fixture. Each nested class contains various cases.
 */
class Inheritance {

    Inheritance() {}

    static class AbstractParent {
        abstract static class Parent {
            @Column("key")
            Integer key;

            @Column("val")
            String val;

            Parent() {}

            Parent(Integer key, String val) {
                this.key = key;
                this.val = val;
            }
        }

        static class Child extends Parent {
            @Column("val2")
            String val2;

            Child() {}

            Child(Integer key, String val, String val2) {
                super(key, val);
                this.val2 = val2;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Child that = (Child) o;
                return Objects.equals(key, that.key)
                        && Objects.equals(this.val, that.val)
                        && Objects.equals(this.val2, that.val2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key, val, val2);
            }
        }
    }

    static class RegularParent {
        static class Parent {
            @Column("key")
            Integer key;

            @Column("val")
            String val;

            Parent() {}

            Parent(Integer key, String val) {
                this.key = key;
                this.val = val;
            }
        }

        static class Child extends Parent {
            @Column("val2")
            String val2;

            Child() {}

            Child(Integer key, String val, String val2) {
                super(key, val);
                this.val2 = val2;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Child that = (Child) o;
                return Objects.equals(key, that.key)
                        && Objects.equals(this.val, that.val)
                        && Objects.equals(this.val2, that.val2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key, val, val2);
            }
        }
    }

    static class MultipleParent {
        static class Parent1 {
            @Column("key")
            Integer key;

            Parent1() {}

            Parent1(Integer key) {
                this.key = key;
            }
        }

        static class Parent2 extends Parent1 {
            @Column("val")
            String val;

            Parent2() {}

            Parent2(Integer key, String val) {
                super(key);
                this.val = val;
            }
        }

        static class Child extends Parent2 {
            @Column("val2")
            String val2;

            Child() {}

            Child(Integer key, String val, String val2) {
                super(key, val);
                this.val2 = val2;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Child that = (Child) o;
                return Objects.equals(key, that.key)
                        && Objects.equals(this.val, that.val)
                        && Objects.equals(this.val2, that.val2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key, val, val2);
            }
        }

    }

    static class ParentWithPrivateField {
        static class Parent {
            @Column("key")
            private Integer key;

            @Column("val")
            private String val;

            Parent() {
            }

            Parent(Integer key, String val) {
                this.key = key;
                this.val = val;
            }

            Integer getKey() {
                return key;
            }

            String getVal() {
                return val;
            }
        }

        static class Child extends Parent {
            @Column("val2")
            String val2;

            Child() {
            }

            Child(Integer key, String val, String val2) {
                super(key, val);
                this.val2 = val2;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Child that = (Child) o;
                return Objects.equals(getKey(), that.getKey())
                        && Objects.equals(this.getVal(), that.getVal())
                        && Objects.equals(this.val2, that.val2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(getKey(), getVal(), val2);
            }
        }
    }

    static class LocalClass {
        Child newChild(Integer key, String val, String val2) {
            return new Child(key, val, val2);
        }

        class Parent {
            @Column("key")
            Integer key;

            @Column("val")
            String val;

            Parent() {
            }

            Parent(Integer key, String val) {
                this.key = key;
                this.val = val;
            }
        }

        class Child extends Parent {
            @Column("val2")
            String val2;

            Child() {
            }

            Child(Integer key, String val, String val2) {
                super(key, val);
                this.val2 = val2;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Child that = (Child) o;
                return Objects.equals(key, that.key)
                        && Objects.equals(this.val, that.val)
                        && Objects.equals(this.val2, that.val2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key, val, val2);
            }
        }
    }

    static class Kv {
        static class Key {
            @Column("key")
            Integer key;

            Key() {}

            Key(Integer key) {
                this.key = key;
            }

            @Override
            public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Key that = (Key) o;
                return Objects.equals(key, that.key);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(key);
            }
        }

        static class ParentV {
            @Column("val")
            String val;

            ParentV() {}

            ParentV(String val) {
                this.val = val;
            }
        }

        static class ChildV extends ParentV {
            @Column("val2")
            String val2;

            ChildV() {}

            ChildV(String val, String val2) {
                super(val);
                this.val2 = val2;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                ChildV that = (ChildV) o;
                return Objects.equals(this.val, that.val)
                        && Objects.equals(this.val2, that.val2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(val, val2);
            }
        }
    }
}
