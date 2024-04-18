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

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.NativeType;

/**
 * Base type represents a {@link NativeType}.
 */
public class BaseTypeSpec extends TypeSpec {

    private final NativeType nativeType;

    /** Creates a basic type with default nullability (non-nullable). */
    BaseTypeSpec(NativeType nativeType) {
        super(false);
        this.nativeType = nativeType;
    }

    /** Creates a basic type with the given nullability. */
    public BaseTypeSpec(NativeType nativeType, boolean nullable) {
        super(nullable);
        // TODO Uncomment his check after https://issues.apache.org/jira/browse/IGNITE-20163 is fixed
        // this.nativeType = Objects.requireNonNull(nativeType, "native type has not been specified.");
        this.nativeType = nativeType;
    }

    /** Returns a native type that this base type represents. */
    public NativeType nativeType() {
        return nativeType;
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
        BaseTypeSpec that = (BaseTypeSpec) o;
        return Objects.equals(nativeType, that.nativeType);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nativeType);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(BaseTypeSpec.class, this, "nullable", isNullable());
    }

}
