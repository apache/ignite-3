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

import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Table;

/**
 * A POJO class representing the whole record.
 */
@Table(Pojo.POJO_RECORD_TABLE_NAME)
class Pojo {
    static final String POJO_RECORD_TABLE_NAME = "pojo_record_table";

    @Id
    Integer id;

    @Id
    @Column(value = "id_str", length = 20)
    String idStr;

    Pojo() {
    }

    Pojo(Integer id, String idStr) {
        this.id = id;
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

        Pojo pojo = (Pojo) o;

        if (id != null ? !id.equals(pojo.id) : pojo.id != null) {
            return false;
        }
        return idStr != null ? idStr.equals(pojo.idStr) : pojo.idStr == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (idStr != null ? idStr.hashCode() : 0);
        return result;
    }
}
