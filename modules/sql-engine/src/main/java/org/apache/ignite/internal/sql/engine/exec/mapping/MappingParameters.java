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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import org.apache.ignite.internal.util.ArrayUtils;

/** Additional information used for mapping. */
public class MappingParameters {

    /** Empty mapping parameters. */
    public static final MappingParameters EMPTY = new MappingParameters(ArrayUtils.OBJECT_EMPTY_ARRAY);

    private final Object[] dynamicParameters;

    /**
     * Creates mapping parameters.
     *
     * @param dynamicParameters Dynamic parameters.
     *
     * @return Mapping parameters.
     */
    public static MappingParameters create(Object[] dynamicParameters) {
        if (dynamicParameters.length == 0) {
            return EMPTY;
        } else {
            return new MappingParameters(dynamicParameters);
        }
    }

    private MappingParameters(Object[] dynamicParameters) {
        this.dynamicParameters = dynamicParameters;
    }

    /** Values of dynamic parameters. */
    public Object[] dynamicParameters() {
        return dynamicParameters;
    }
}
