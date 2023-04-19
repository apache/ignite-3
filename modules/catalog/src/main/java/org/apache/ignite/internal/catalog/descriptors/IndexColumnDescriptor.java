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

package org.apache.ignite.internal.catalog.descriptors;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * Indexed column descriptor.
 */
public class IndexColumnDescriptor implements Serializable {
    private static final long serialVersionUID = 5750677168056750717L;

    private final String name;

    private final ColumnCollation collation;

    public IndexColumnDescriptor(String name, ColumnCollation collation) {
        this.name = name;
        this.collation = Objects.requireNonNull(collation, "collation");
    }

    public String name() {
        return name;
    }

    public ColumnCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
