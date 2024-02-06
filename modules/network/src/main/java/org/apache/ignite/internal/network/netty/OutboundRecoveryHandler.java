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

package org.apache.ignite.internal.network.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;

/** Outbound handler that adds outgoing message to the recovery descriptor. */
public class OutboundRecoveryHandler extends ChannelOutboundHandlerAdapter {
    /** Handler name. */
    public static final String NAME = "outbound-recovery-handler";

    /** Recovery descriptor. */
    private final RecoveryDescriptor descriptor;

    /**
     * Constructor.
     *
     * @param descriptor Recovery descriptor.
     */
    public OutboundRecoveryHandler(RecoveryDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        OutNetworkObject outNetworkObject = (OutNetworkObject) msg;

        if (outNetworkObject.shouldBeSavedForRecovery()) {
            descriptor.add(outNetworkObject);
        }

        super.write(ctx, msg, promise);
    }
}
