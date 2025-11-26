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

package org.apache.ignite.internal.network;

import org.apache.ignite.internal.network.direct.DirectMessageReader;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.serialization.MessageFormat;
import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageWriter;

/**
 * Naive message format that uses direct serialization and deserialization. Both nodes must be of the same version for this format to work.
 *
 * <p>This is the default message format used in Ignite.
 */
public class NaiveMessageFormat implements MessageFormat {
    @Override
    public MessageWriter writer(MessageSerializationRegistry serializationRegistry, byte protoVer) {
        return new DirectMessageWriter(serializationRegistry, protoVer);
    }

    @Override
    public MessageReader reader(MessageSerializationRegistry serializationRegistry, byte protoVer) {
        return new DirectMessageReader(serializationRegistry, protoVer);
    }
}
