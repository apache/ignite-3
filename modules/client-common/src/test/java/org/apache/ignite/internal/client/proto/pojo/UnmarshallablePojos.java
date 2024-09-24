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

package org.apache.ignite.internal.client.proto.pojo;

/** POJO with incorrect fields. */
public class UnmarshallablePojos {
    /** Pojo with unsupported type. */
    public static class UnsupportedType {
        // Unsupported type
        Object obj;
    }

    /** Pojo with private field. */
    public static class PrivateField {
        // Private field
        private int intField;

        @Override
        public String toString() {
            return "intField=" + intField;
        }
    }

    /** Pojo with static field. */
    public static class StaticField {
        // Public but static field
        public static long longField;
    }

    /** Pojo with invalid getter name. */
    public static class InvalidGetterName {
        // Getter not starting with "get"
        public int intField() {
            return 0;
        }
    }

    /** Pojo with private getter. */
    public static class PrivateGetter {
        // Private getter
        private int getI() {
            return 0;
        }
    }
}
