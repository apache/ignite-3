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

package org.apache.ignite.network.internal.netty;

import java.util.concurrent.CompletableFuture;

public abstract class NettyChannel {

    abstract CompletableFuture<NettySender> channel();

    abstract boolean isOpen();

    public static NettyChannel fromServer(NettySender sender) {
        return new ServerChannel(sender);
    }

    public static NettyChannel fromFuture(CompletableFuture<NettySender> fut) {
        return new FutureChannel(fut);
    }

    public abstract void close();

    private static class ServerChannel extends NettyChannel {

        private final NettySender sender;

        private ServerChannel(NettySender sender) {
            this.sender = sender;
        }


        /** {@inheritDoc} */
        @Override boolean isOpen() {
            return true;
        }

        @Override public void close() {
            sender.close();
        }

        /** {@inheritDoc} */
        @Override CompletableFuture<NettySender> channel() {
            return CompletableFuture.completedFuture(sender);
        }
    }

    private static class FutureChannel extends NettyChannel {

        private final CompletableFuture<NettySender> fut;

        private FutureChannel(CompletableFuture<NettySender> fut) {
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override CompletableFuture<NettySender> channel() {
            return fut;
        }

        @Override boolean isOpen() {
            return fut.isDone() && !fut.isCompletedExceptionally();
        }

        @Override public void close() {
            fut.whenComplete((sender, throwable) -> {
               if (sender != null)
                   sender.close();
            });
        }
    }

}
