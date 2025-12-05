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
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * Declaration variants fixture. Each nested class contains record and its class alternative declarations.
 */
class Records {
    private Records() {}

    static final String SQL_PATTERN = "CREATE TABLE {} (key INT PRIMARY KEY, val VARCHAR, val2 VARCHAR)";

    static final SchemaDescriptor schema = new SchemaDescriptor(1,
            new org.apache.ignite.internal.schema.Column[]{
                    new org.apache.ignite.internal.schema.Column("KEY", NativeTypes.INT32, false)
            },
            new org.apache.ignite.internal.schema.Column[]{
                    new org.apache.ignite.internal.schema.Column("VAL", NativeTypes.STRING, true),
                    new org.apache.ignite.internal.schema.Column("VAL2", NativeTypes.STRING, true)
            });

    static class ComponentsExact {
        record RecordK(@Column("key") Integer id) { }
        record RecordV(@Column("val") String val) { }
        record Record(@Column("key") Integer id, @Column("val") String val) { }

        record ExplicitCanonical(@Column("key") Integer id, @Column("val") String val) {
            ExplicitCanonical(@Column("key") Integer id, @Column("val") String val) {
                this.id = id;
                this.val = val;
                if (id == null && val == null) {
                    throw new IllegalArgumentException("null args");
                }
            }
        }

        record ExplicitCanonicalV(@Column("val") String val) {
            ExplicitCanonicalV(@Column("val") String val) {
                this.val = val;
                if (val == null) {
                    throw new IllegalArgumentException("null args");
                }
            }
        }

        public record ExplicitCompact(@Column("key") Integer id, @Column("val") String val) {
            public ExplicitCompact {
                if (id == null && val == null) {
                    throw new IllegalArgumentException("null args");
                }
            }
        }

        public record ExplicitCompactV(@Column("val") String val) {
            public ExplicitCompactV {
                if (val == null) {
                    throw new IllegalArgumentException("null args");
                }
            }
        }

        record ExplicitMultiple(@Column("key") Integer id, @Column("val") String val) {
            ExplicitMultiple(Integer id) {
                this(id, "");
            }

            ExplicitMultiple(String val) {
                this(-1, val);
            }
        }

        record ExplicitMultipleV(@Column("val") String val) {
            ExplicitMultipleV(Integer unused, String val) {
                this(val);
            }

            ExplicitMultipleV(String val, Integer unused) {
                this(val);
            }
        }

        record ExplicitNoArgs(@Column("key") Integer id, @Column("val") String val) {
            ExplicitNoArgs() {
                this(-1, "no-arg");
            }
        }

        record ExplicitNoArgsV(@Column("val") String val) {
            ExplicitNoArgsV() {
                this("no-arg");
            }
        }

        static final class Class {
            @Column("key")
            private Integer id;

            @Column("val")
            private String val;

            Class() {}

            Class(Integer id, String val) {
                this.id = id;
                this.val = val;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (Class) obj;
                return Objects.equals(this.id, that.id)
                        && Objects.equals(this.val, that.val);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, val);
            }
        }

        static final class ClassK {
            @Column("key")
            private Integer id;

            ClassK() {}

            ClassK(Integer id) {
                this.id = id;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (ClassK) obj;
                return Objects.equals(this.id, that.id);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id);
            }
        }

        static final class ClassV {
            @Column("val")
            private String val;

            ClassV() {}

            ClassV(String val) {
                this.val = val;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (ClassV) obj;
                return Objects.equals(this.val, that.val);
            }

            @Override
            public int hashCode() {
                return Objects.hash(val);
            }
        }
    }

    static class ComponentsWide {
        record Record(@Column("key") Integer id, @Column("val") String val, @Column("val2") String val2, @Column("val3") String val3) { }

        static final class Class {
            @Column("key")
            private Integer id;
            @Column("val")
            private String val;
            @Column("val2")
            private String val2;
            @Column("val3")
            private String val3;

            Class() {}

            Class(Integer id, String val, String val2, String val3) {
                this.id = id;
                this.val = val;
                this.val2 = val2;
                this.val3 = val3;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (Class) obj;
                return Objects.equals(this.id, that.id)
                        && Objects.equals(this.val, that.val)
                        && Objects.equals(this.val2, that.val2)
                        && Objects.equals(this.val3, that.val3);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, val, val2, val3);
            }
        }
    }

    static class ComponentsNarrow {
        record Record(@Column("key") Integer id) { }

        static final class Class {
            @Column("key")
            private Integer id;

            Class() {}

            Class(Integer id) {
                this.id = id;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (Class) obj;
                return Objects.equals(this.id, that.id);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id);
            }
        }
    }

    static class ComponentsReordered {
        record Record(@Column("val") String val, @Column("key") Integer id) { }

        static final class Class {
            @Column("val")
            private String val;
            @Column("key")
            private Integer id;

            Class() {}

            Class(@Column("val") String val, @Column("key") Integer id) {
                this.val = val;
                this.id = id;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (Class) obj;
                return Objects.equals(this.val, that.val) &&
                        Objects.equals(this.id, that.id);
            }

            @Override
            public int hashCode() {
                return Objects.hash(val, id);
            }
        }
    }

    static class ComponentsWrongTypes {
        record RecordK(@Column("key") Short id) { }
        record RecordV(@Column("val") Integer val) { }
        record Record(@Column("key") Short id, @Column("val") Integer val) { }

        static final class Class {
            @Column("key")
            private Short id;
            @Column("val")
            private Integer val;

            Class() {}

            Class(Short id, Integer val) {
                this.id = id;
                this.val = val;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (Class) obj;
                return Objects.equals(this.id, that.id) &&
                        Objects.equals(this.val, that.val);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, val);
            }
        }

        static final class ClassK {
            @Column("key")
            private Short id;

            ClassK() {}

            ClassK(Short id) {
                this.id = id;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (ClassK) obj;
                return Objects.equals(this.id, that.id);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id);
            }
        }

        static final class ClassV {
            @Column("val")
            private Integer val;

            ClassV() {}

            ClassV(Integer id) {
                this.val = id;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (ClassV) obj;
                return Objects.equals(this.val, that.val);
            }

            @Override
            public int hashCode() {
                return Objects.hash(val);
            }
        }
    }

    static class ComponentsEmpty {
        record Record() { }
        static final class Class {}
    }

    static class NotAnnotatedNotMapped {
        record Record(Integer id, String value) { }

        static final class Class {
            private final Integer id;
            private final String value;

            Class(Integer id, String value) {
                this.id = id;
                this.value = value;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }
                if (obj == null || obj.getClass() != this.getClass()) {
                    return false;
                }
                var that = (Class) obj;
                return Objects.equals(this.id, that.id) &&
                        Objects.equals(this.value, that.value);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, value);
            }
        }
    }

    static class NoDefaultConstructor {
        static final class Class {
            @Column("key")
            private Integer key;
            @Column("val")
            private String val;

            Class(Integer key, String val) {
                this.key = key;
                this.val = val;
            }
        }

        static final class ClassV {
            @Column("val")
            private String val;

            ClassV(String val) {
                this.val = val;
            }
        }
    }
}
