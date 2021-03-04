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

package org.apache.ignite.table;

import org.apache.ignite.table.binary.BinaryObject;

/**
 * Binary row interface.
 */
public interface BinaryRow { //TODO: Extends BinaryObject interface?
    /**
     * Gets column value for given column name.
     *
     * @param colName Column name.
     * @param <T> Value type.
     * @return Column value.
     */
    public <T> T value(String colName);

    /**
     * Gets binary object column.
     *
     * @param colName Column name.
     * @return Column value.
     */
    public BinaryObject binaryObjectField(String colName);

    /**
     * Gets {@code byte} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    byte byteValue(String colName);

    /**
     * Gets {@code short} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    short shortValue(String colName);

    /**
     * Gets {@code int} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    int intValue(String colName);

    /**
     * Gets {@code long} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    long longValue(String colName);

    /**
     * Gets {@code float} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    float floatValue(String colName);

    /**
     * Gets {@code double} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    double doubleValue(String colName);

    /**
     * Gets {@code String} object column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    String stringValue(String colName);
}
