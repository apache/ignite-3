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

package org.apache.ignite.internal.network.messages;

/** Test enum. */
public enum TestEnum {
    ONE(0),
    TWO(1),
    THREE(2);

    private final int id;

    TestEnum(int id) {
        this.id = id;
    }

    /** Cached array with all enum values. */
    private static final TestEnum[] VALUES = values();

    /**
     * Returns the enumerated value from its id.
     *
     * @param id Id of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by id.
     */
    public static TestEnum fromId(int id) throws IllegalArgumentException {
        if (id < 0) {
            throw new IllegalArgumentException("No enum constant from id: " + id);
        }

        for (TestEnum testEnum : VALUES) {
            if (testEnum.id == id) {
                return testEnum;
            }
        }
        throw new IllegalArgumentException("No enum constant from id: " + id);
    }

    public int id() {
        return id;
    }
}
