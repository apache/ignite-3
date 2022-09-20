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

package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;

/**
 * Simple result definition of statement execution, backed by byte[] array.
 */
public class StatementResultInfo implements Serializable {
    /** Result data. */
    private final byte[] res;

    /**
     * Constructs result definition from the byte array.
     *
     * @param res byte array.
     */
    public StatementResultInfo(byte[] res) {
        this.res = res;
    }

    /**
     * Returns result as row byte array.
     *
     * @return result as row byte array.
     */
    public byte[] result() {
        return res;
    }

}
