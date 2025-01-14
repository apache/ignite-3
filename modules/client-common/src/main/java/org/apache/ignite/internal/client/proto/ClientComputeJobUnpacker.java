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

package org.apache.ignite.internal.client.proto;

import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ComputeJobDataType;
import org.apache.ignite.internal.compute.SharedComputeUtils;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.jetbrains.annotations.Nullable;

/** Unpacks job results. */
public final class ClientComputeJobUnpacker {
    /**
     * Unpacks compute job result. If the marshaller is provided, it will be used to unmarshal the result. If the marshaller is not provided
     * and the result class is provided and the result is a tuple, it will be unpacked as a pojo of that class. If the marshaller is not
     * provided and the result is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param unpacker Unpacker.
     * @param marshaller Marshaller.
     * @param resultClass Result class.
     * @return Unpacked result.
     */
    public static @Nullable Object unpackJobResult(
            ClientMessageUnpacker unpacker,
            @Nullable Marshaller<?, byte[]> marshaller,
            @Nullable Class<?> resultClass
    ) {
        ComputeJobDataHolder holder = unpackJobArgumentWithoutMarshaller(unpacker);

        return SharedComputeUtils.unmarshalArgOrResult(holder, marshaller, resultClass);
    }

    /** Unpacks compute job argument without marshaller. */
    public static @Nullable ComputeJobDataHolder unpackJobArgumentWithoutMarshaller(ClientMessageUnpacker unpacker) {
        if (unpacker.tryUnpackNil()) {
            return null;
        }

        int typeId = unpacker.unpackInt();
        ComputeJobDataType type = ComputeJobDataType.fromId(typeId);
        if (type == null) {
            throw new UnmarshallingException("Unsupported compute job type id: " + typeId);
        }

        return new ComputeJobDataHolder(type, unpacker.readBinary());
    }
}
