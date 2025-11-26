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

package org.apache.ignite.migrationtools.tests.models;

import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/** Provides two models with intercepting field names. */
public class InterceptingFieldsModel {
    /** Key model. */
    public static class Key {
        private int key1;

        private int key2;

        public Key() {
            // No-op
        }

        /**
         * Constructor.
         *
         * @param key1 key1
         * @param key2 key2
         */
        public Key(int key1, int key2) {
            this.key1 = key1;
            this.key2 = key2;
        }

        public int key1() {
            return key1;
        }

        public int key2() {
            return key2;
        }
    }

    /** Value model. */
    public static class Value {
        @QuerySqlField
        private long key1;

        @QuerySqlField
        private long key2;

        @QuerySqlField
        private String value;

        public Value() {
            // No-op.
        }

        /**
         * Default constructor.
         *
         * @param key1 key1
         * @param key2 key2
         * @param value val
         */
        public Value(long key1, long key2, String value) {
            this.key1 = key1;
            this.key2 = key2;
            this.value = value;
        }

        public long key1() {
            return key1;
        }

        public long key2() {
            return key2;
        }

        public String value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value value1 = (Value) o;
            return key1 == value1.key1 && key2 == value1.key2 && Objects.equals(value, value1.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key1, key2, value);
        }
    }
}
