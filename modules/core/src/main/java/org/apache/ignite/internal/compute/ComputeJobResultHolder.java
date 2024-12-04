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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

/**
 * Holds marshalled binary data for the compute, used for the optimization to reduce number of conversions in case when thin client requests
 * job execution on another node. In this case, the argument (POJO or a Tuple) is packed on the client, unpacked as a byte array on the
 * client handler node, wrapped in the {@link ComputeJobArgumentHolder}, passed on the remote node and is unmarshalled just before the job
 * execution. If the argument was passed in this way, then the result of the job is marshalled to the byte buffer on the target node,
 * wrapped in this object, passed back to the client handler node, packed as a byte buffer, returned to the client and unpacked there.
 */
public class ComputeJobResultHolder implements Serializable {
    private static final long serialVersionUID = -2223356265028226195L;

    private ComputeJobDataType type;

    private ByteBuffer data;

    /**
     * Constructs a holder from a byte array and wraps it into the {@link ByteBuffer}.
     *
     * @param type Job argument type.
     * @param data Marshalled data.
     */
    public ComputeJobResultHolder(ComputeJobDataType type, byte[] data) {
        this.type = type;
        this.data = ByteBuffer.wrap(data);
    }

    /**
     * Constructs a holder from a buffer.
     *
     * @param type Job argument type.
     * @param data Marshalled data.
     */
    public ComputeJobResultHolder(ComputeJobDataType type, ByteBuffer data) {
        this.type = type;
        this.data = data;
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
    public ByteBuffer data() {
        return data;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(type.id());
        out.writeInt(data.remaining());
        Channels.newChannel(out).write(data);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.type = ComputeJobDataType.fromId(in.readInt());
        this.data = ByteBuffer.allocate(in.readInt());
        Channels.newChannel(in).read(data);
    }
}
