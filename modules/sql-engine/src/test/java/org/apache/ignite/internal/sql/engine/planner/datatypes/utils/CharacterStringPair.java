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

package org.apache.ignite.internal.sql.engine.planner.datatypes.utils;

import org.apache.ignite.internal.type.NativeType;


/**
 * Enumerates possible character string type pairs for test purposes.
 *
 * <p>This is an upper triangular matrix of numeric, where types are enumerated from
 * lesser (VARCHAR(1)) to greater (VARCHAR(5)). That is, this enumeration contains
 * pair (VARCHAR(1), VARCHAR(5)), but not (VARCHAR(5), VARCHAR(1)).
 */
@SuppressWarnings("NonSerializableFieldInSerializableClass")
public enum CharacterStringPair implements TypePair {
    VARCHAR_1_VARCHAR_1(Types.VARCHAR_1, Types.VARCHAR_1),
    VARCHAR_1_VARCHAR_3(Types.VARCHAR_1, Types.VARCHAR_3),
    VARCHAR_1_VARCHAR_5(Types.VARCHAR_1, Types.VARCHAR_5),

    VARCHAR_3_VARCHAR_3(Types.VARCHAR_3, Types.VARCHAR_3),
    VARCHAR_3_VARCHAR_5(Types.VARCHAR_3, Types.VARCHAR_5),

    VARCHAR_5_VARCHAR_5(Types.VARCHAR_5, Types.VARCHAR_5),
    ;

    private final NativeType first;
    private final NativeType second;

    CharacterStringPair(NativeType first, NativeType second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public NativeType first() {
        return first;
    }

    @Override
    public NativeType second() {
        return second;
    }
}
