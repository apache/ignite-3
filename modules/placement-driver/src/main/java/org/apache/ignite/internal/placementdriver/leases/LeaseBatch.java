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

package org.apache.ignite.internal.placementdriver.leases;

import static org.apache.ignite.internal.util.IgniteUtils.bytesToList;
import static org.apache.ignite.internal.util.IgniteUtils.collectionToBytes;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Representation of leases batch.
 */
public class LeaseBatch {
    List<Lease> leases;

    public LeaseBatch(List<Lease> leases) {
        this.leases = leases;
    }

    public List<Lease> leases() {
        return leases;
    }

    public byte[] bytes() {
        return collectionToBytes(leases, Lease::bytes);
    }

    public static LeaseBatch fromBytes(byte[] bytes) {
        return new LeaseBatch(bytesToList(bytes, Lease::fromBytes));
    }
}
