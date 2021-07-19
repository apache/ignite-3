/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.internal.table;

public class ClientColumn {
    private final String name;

    private final String type;

    private final boolean nullable;

    private final boolean isKey;

    private final int schemaIndex;

    public ClientColumn(String name, String type, boolean nullable, boolean isKey, int schemaIndex) {
        assert name != null;
        assert schemaIndex >= 0;

        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.isKey = isKey;
        this.schemaIndex = schemaIndex;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public boolean nullable() {
        return nullable;
    }

    public boolean key() {
        return isKey;
    }

    public int schemaIndex() {
        return schemaIndex;
    }
}
