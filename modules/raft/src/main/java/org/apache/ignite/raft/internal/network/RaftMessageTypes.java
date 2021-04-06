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

package org.apache.ignite.raft.internal.network;

// TODO sanpwc: Uncomment when message generation will be ready.
//@DirectlyMarshallableTypes(moduleType = 10)
public enum RaftMessageTypes {
    // TODO sanpwc: Clarity message type names.
    REQUEST1((short) 1000),

    REQUEST2((short) 1001),

    REQUEST3((short) 1005),

    REQUEST4((short) 1006),

    REQUEST5((short) 1009);

    private short msgType;

    RaftMessageTypes(short msgType) {
        this.msgType = msgType;
    }

    public short msgType() {
        return msgType;
    }
}
