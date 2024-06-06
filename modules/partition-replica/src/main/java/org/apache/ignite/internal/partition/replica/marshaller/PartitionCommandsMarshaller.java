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

package org.apache.ignite.internal.partition.replica.marshaller;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.raft.Marshaller;

/**
 * {@link Marshaller} that first writes some metadata about an object and then it writes the actual serialized
 * representation of the object.
 */
public interface PartitionCommandsMarshaller extends Marshaller {
    /**
     * Used instead of a required catalog version when there is no requirement.
     */
    int NO_VERSION_REQUIRED = -1;

    /**
     * Reads required catalog version from the provided buffer.
     *
     * @param raw Buffer to read from.
     * @return Catalog version. {@value #NO_VERSION_REQUIRED} if version is not required for the given command.
     */
    int readRequiredCatalogVersion(ByteBuffer raw);
}
