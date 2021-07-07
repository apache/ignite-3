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

package org.apache.ignite.clientconnector;

import java.util.BitSet;

class ClientContext {
    private final int verMajor;

    private final int verMinor;

    private final int verPatch;

    private final int clientCode;

    private final BitSet features;

    public ClientContext(int verMajor, int verMinor, int verPatch, int clientCode, BitSet features) {
        this.verMajor = verMajor;
        this.verMinor = verMinor;
        this.verPatch = verPatch;
        this.clientCode = clientCode;
        this.features = features;
    }

    @Override
    public String toString() {
        return "ClientContext{" +
                "verMajor=" + verMajor +
                ", verMinor=" + verMinor +
                ", verPatch=" + verPatch +
                ", clientCode=" + clientCode +
                ", features=" + features +
                '}';
    }
}
