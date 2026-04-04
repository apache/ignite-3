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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;

import java.util.Objects;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.catalog.annotations.ColumnRef;
import org.apache.ignite.catalog.annotations.Index;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.catalog.annotations.Zone;

/**
 * A POJO class representing the whole record with inheritance.
 */
@Table(
        value = ItCatalogDslTest.POJO_RECORD_EXTENDED_TABLE_NAME,
        zone = @Zone(value = ItCatalogDslTest.ZONE_NAME, storageProfiles = DEFAULT_AIPERSIST_PROFILE_NAME),
        colocateBy = @ColumnRef("id"),
        indexes = @Index(value = "ix_pojo", columns = {
                @ColumnRef("f_name"),
                @ColumnRef(value = "l_name", sort = SortOrder.DESC),
                @ColumnRef(value = "f_name_extended")
        })
)
class PojoExtended extends Pojo {
    @Column("f_name_extended")
    String firstNameExtended;

    PojoExtended() {}

    PojoExtended(Integer id, String idStr, String firstName, String lastName, String str, String firstNameExtended) {
        super(id, idStr, firstName, lastName, str);
        this.firstNameExtended = firstNameExtended;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PojoExtended pojo = (PojoExtended) o;
        return Objects.equals(id, pojo.id)
                && Objects.equals(idStr, pojo.idStr)
                && Objects.equals(firstName, pojo.firstName)
                && Objects.equals(lastName, pojo.lastName)
                && Objects.equals(str, pojo.str)
                && Objects.equals(firstNameExtended, pojo.firstNameExtended);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, idStr, firstName, lastName, str, firstNameExtended);
    }
}
