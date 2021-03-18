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

package org.apache.ignite.network.scalecube;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.network.MessageMapper;
import org.apache.ignite.network.MessageMappingException;

/**
 * Mapper for {@link TestRequest}.
 */
public class TestRequestMapper implements MessageMapper<TestRequest> {
    /** {@inheritDoc} */
    @Override public TestRequest readMessage(ObjectInputStream objectInputStream) throws MessageMappingException {
        try {
            final int number = objectInputStream.readInt();
            return new TestRequest(number);
        }
        catch (IOException e) {
            throw new MessageMappingException("Failed to deserialize", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeMessage(TestRequest message, ObjectOutputStream objectOutputStream) throws MessageMappingException {
        try {
            objectOutputStream.writeInt(message.number());
        }
        catch (IOException e) {
            throw new MessageMappingException("Failed to serialize", e);
        }
    }
}
