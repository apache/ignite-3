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

package org.apache.ignite.internal.jdbc.proto.event;

import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.proto.ClientMessage;

public class JdbcFinishTxRequest implements ClientMessage {
    private long connectionId;
    private boolean commit;

    public JdbcFinishTxRequest() {
    }

    public JdbcFinishTxRequest(long connectionId, boolean commit) {
        this.connectionId = connectionId;
        this.commit = commit;
    }
    
    public long connectionId() {
        return connectionId;
    }
    
    public boolean commit() {
        return commit;
    }

    @Override
    public void writeBinary(ClientMessagePacker packer) {
        packer.packLong(connectionId);
        packer.packBoolean(commit);
    }

    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        connectionId = unpacker.unpackLong();
        commit = unpacker.unpackBoolean();
    }
}
