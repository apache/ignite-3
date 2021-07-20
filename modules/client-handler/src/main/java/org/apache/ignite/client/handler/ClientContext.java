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

package org.apache.ignite.client.handler;

import java.util.BitSet;

/**
 * Client connection context.
 */
class ClientContext {
    /** Major version part. */
    private final int verMajor;

    /** Minor version part. */
    private final int verMinor;

    /** Patch version part. */
    private final int verPatch;

    /** Client type code. */
    private final int clientCode;

    /** Feature set. */
    private final BitSet features;

    /**
     * Constructor.
     *
     * @param verMajor Major version part.
     * @param verMinor Minor version part.
     * @param verPatch Patch version part.
     * @param clientCode Client type code.
     * @param features Feature set.
     */
    ClientContext(int verMajor, int verMinor, int verPatch, int clientCode, BitSet features) {
        this.verMajor = verMajor;
        this.verMinor = verMinor;
        this.verPatch = verPatch;
        this.clientCode = clientCode;
        this.features = features;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ClientContext{" +
                "verMajor=" + verMajor +
                ", verMinor=" + verMinor +
                ", verPatch=" + verPatch +
                ", clientCode=" + clientCode +
                ", features=" + features +
                '}';
    }
}
