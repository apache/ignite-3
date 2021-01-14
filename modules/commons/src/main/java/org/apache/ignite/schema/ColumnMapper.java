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

package org.apache.ignite.schema;

/**
 * Node local mapper defines mapping rules for current available key/value class version.
 *
 * Note: When you change your class, you may want to upgrade the mapper as well, and vice versa.
 *
 * Note: Mapper is configured on per-node basis and allows to have different versions of the same class
 * on different nodes. This makes possible smooth schema upgrade with changing user key-value classes via
 * rolling restart.
 *
 * Note: Data and schema consistency is fully determined with schema mode and user actions order,
 * and have nothing to do with the mapper.
 */
public interface ColumnMapper {
    /**
     * Gets column name mapped to field name.
     *
     * @param columnName Field name.
     * @return Field name.
     */
    String columnToField(String columnName);
}
