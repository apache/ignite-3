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

package org.apache.ignite.internal.sql.engine.exec.row;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * Row type is a type that consists of a list of other {@link TypeSpec}s.
 */
public class RowType extends TypeSpec {

    private final List<TypeSpec> fields;

    /** Creates a row type with the given nullability. */
    public RowType(List<TypeSpec> fields, boolean nullable) {
        super(nullable);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Field list can not be empty");
        }
        this.fields = fields;
    }

    /** A list of types this row consists of. */
    public List<TypeSpec> fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType that = (RowType) o;
        return Objects.equals(fields, that.fields);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RowType.class, this, "nullable", isNullable());
    }
}
