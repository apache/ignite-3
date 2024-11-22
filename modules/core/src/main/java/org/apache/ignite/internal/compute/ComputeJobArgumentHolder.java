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

package org.apache.ignite.internal.compute;

/**
 * Holds unmarshalled binary data for the compute, used for the optimization to reduce number of conversions in case when thin client
 * requests job execution on another job. In this case, the argument is packed on the client, unpacked as a binary data on the client
 * handler node, passed as an object of this class to the remote node and only there it is unmarshalled.
 */
public class ComputeJobArgumentHolder {
    private final ComputeJobType type;

    private final byte[] data;

    /**
     * Constructs a holder.
     *
     * @param type Job argument type.
     * @param data Marshalled data.
     */
    public ComputeJobArgumentHolder(ComputeJobType type, byte[] data) {
        this.type = type;
        this.data = data;
    }

    /**
     * Returns job argument type.
     *
     * @return Job argument type.
     */
    public ComputeJobType type() {
        return type;
    }

    /**
     * Returns marshalled data.
     *
     * @return Marshalled data.
     */
    public byte[] data() {
        return data;
    }
}
