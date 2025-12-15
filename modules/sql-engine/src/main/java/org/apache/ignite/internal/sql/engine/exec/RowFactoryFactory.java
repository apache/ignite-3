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

package org.apache.ignite.internal.sql.engine.exec;

import org.apache.ignite.internal.type.StructNativeType;

/**
 * A factory-of-factories interface responsible for creating {@link RowFactory} instances based on a provided {@link StructNativeType}.
 *
 * <p>This abstraction allows components to supply row factories that are specifically tailored to the schema or layout represented by the
 * given native type.
 *
 * @param <RowT> The type of row objects produced by the resulting {@link RowFactory}.
 */
@FunctionalInterface
public interface RowFactoryFactory<RowT> {
    /**
     * Creates a new {@link RowFactory} instance configured according to the provided {@link StructNativeType}.
     *
     * @param type Layout of the rows being produced by resulting factory; must not be {@code null}.
     * @return A {@link RowFactory} producing rows of type {@code RowT} with regard to provided layout.
     */
    RowFactory<RowT> create(StructNativeType type);
}
