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

package org.apache.ignite.internal.client.proto;

import org.apache.ignite.sql.ColumnType;

public class JobArgumentTypeId {
    private final int id;
    private final Type type;

    JobArgumentTypeId(int id) {
       this.id = id;
       this.type = Type.fromId(id);
    }

    public int id() {
        return id;
    }

    public Type type() {
        return type;
    }

    public static enum Type {
        NATIVE, MARSHALLED_TUPLE;

        static Type  fromId(int id) {
            ColumnType nativeType = ColumnType.getById(id);
            if (nativeType == null) {
                return MARSHALLED_TUPLE;
            }

            return NATIVE;
        }
    }

}
