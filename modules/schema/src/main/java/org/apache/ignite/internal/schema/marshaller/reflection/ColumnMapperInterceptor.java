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

package org.apache.ignite.internal.schema.marshaller.reflection;

/**
 * An interceptor provides method for additional transformation of a column data on before write/after read.
 *
 * @param <TargetT> Object type.
 * @param <ColumnT> Column type.
 */
public interface ColumnMapperInterceptor<TargetT, ColumnT> {
    /**
     * Transforms object to a column type. Called before write data to a column.
     *
     * @param obj Object to transform.
     * @return Data to write.
     * @throws Exception If transformation failed.
     */
    ColumnT beforeWrite(TargetT obj) throws Exception;

    /**
     * Transforms to an object of the target type. Called after data read from a column.
     *
     * @param data Column data.
     * @return Object of the target type.
     * @throws Exception If transformation failed.
     */
    TargetT afterRead(ColumnT data) throws Exception;
}
