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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.app.PojoValue.POJO_KEY_VALUE_TABLE_NAME;

import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Table;

/**
 * A POJO class representing just value (i.e. everything except key) columns of a table.
 */
@Table(POJO_KEY_VALUE_TABLE_NAME)
class PojoValue {
    static final String POJO_KEY_VALUE_TABLE_NAME = "pojo_key_value_table";

    @Column(value = "id_str", length = 20)
    String idStr;

    PojoValue() {
    }

    PojoValue(String idStr) {
        this.idStr = idStr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PojoValue pojoValue = (PojoValue) o;

        return idStr != null ? idStr.equals(pojoValue.idStr) : pojoValue.idStr == null;
    }

    @Override
    public int hashCode() {
        return idStr != null ? idStr.hashCode() : 0;
    }
}
