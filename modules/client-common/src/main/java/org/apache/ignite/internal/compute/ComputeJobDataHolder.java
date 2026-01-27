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

import org.jetbrains.annotations.Nullable;

/**
 * Holds marshalled binary data for the compute, used for the optimization to reduce number of conversions in case when thin client requests
 * job execution on another node. In this case, the argument (POJO or a Tuple) is packed on the client, unpacked as a byte array on the
 * client handler node, wrapped in this object, passed on the remote node and is unmarshalled just before the job execution. If the argument
 * was passed in this way, then the result of the job is marshalled to the byte array on the target node, wrapped in this object, passed
 * back to the client handler node, packed as a byte array, returned to the client and unpacked there.
 */
public class ComputeJobDataHolder {
    private final ComputeJobDataType type;

    private final byte @Nullable [] data;

    private final @Nullable Long observableTimestamp;

    /**
     * Constructs a holder.
     *
     * @param type Job argument type.
     * @param data Marshalled data.
     */
    public ComputeJobDataHolder(ComputeJobDataType type, byte @Nullable [] data, @Nullable Long observableTimestamp) {
        this.type = type;
        this.data = data;
        this.observableTimestamp = observableTimestamp;
    }

    /**
     * Returns job argument type.
     *
     * @return Job argument type.
     */
    public ComputeJobDataType type() {
        return type;
    }

    /**
     * Returns marshalled data.
     *
     * @return Marshalled data.
     */
    public byte @Nullable [] data() {
        return data;
    }

    /**
     * Returns observable timestamp.
     *
     * @return Observable timestamp.
     */
    public @Nullable Long observableTimestamp() {
        return observableTimestamp;
    }
}
