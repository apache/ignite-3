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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jetbrains.annotations.Async.Execute;
import org.jetbrains.annotations.Async.Schedule;

/**
 * Netty utilities.
 */
public class NettyUtils {
    /**
     * Convert a Netty {@link Future} to a {@link CompletableFuture}.
     *
     * @param nettyFuture Netty future.
     * @param mapper      Function that maps successfully resolved Netty future to a value for a CompletableFuture.
     * @param <T>         Resulting future type.
     * @param <R>         Netty future result type.
     * @param <F>         Netty future type.
     * @return CompletableFuture.
     */
    public static <T, R, F extends Future<R>> CompletableFuture<T> toCompletableFuture(
            @Schedule F nettyFuture,
            Function<F, T> mapper
    ) {
        return toCompletableFuture(nettyFuture, mapper, CompletableFuture::new);
    }

    /**
     * Convert a Netty {@link Future} to a {@link CompletableFuture}.
     *
     * @param nettyFuture Netty future.
     * @param mapper      Function that maps successfully resolved Netty future to a value for a CompletableFuture.
     * @param completableFutureFactory Factory used to produce a fresh instance of a {@link CompletableFuture}.
     * @param <T>         Resulting future type.
     * @param <R>         Netty future result type.
     * @param <F>         Netty future type.
     * @return CompletableFuture.
     */
    public static <T, R, F extends Future<R>> CompletableFuture<T> toCompletableFuture(
            @Schedule F nettyFuture,
            Function<F, T> mapper,
            Supplier<? extends CompletableFuture<T>> completableFutureFactory
    ) {
        CompletableFuture<T> completableFuture = completableFutureFactory.get();

        nettyFuture.addListener((@Execute F doneFuture) -> {
            if (doneFuture.isSuccess()) {
                completableFuture.complete(mapper.apply(doneFuture));
            } else if (doneFuture.isCancelled()) {
                completableFuture.cancel(true);
            } else {
                completableFuture.completeExceptionally(doneFuture.cause());
            }
        });

        return completableFuture;
    }

    /**
     * Convert a Netty {@link Future} to a {@link CompletableFuture}.
     *
     * @param <T>    Type of the future.
     * @param future Future.
     * @return CompletableFuture.
     */
    public static <T> CompletableFuture<T> toCompletableFuture(Future<T> future) {
        return toCompletableFuture(future, fut -> null);
    }

    /**
     * Convert a Netty {@link ChannelFuture} to a {@link CompletableFuture}.
     *
     * @param channelFuture Channel future.
     * @return CompletableFuture.
     */
    public static CompletableFuture<Channel> toChannelCompletableFuture(ChannelFuture channelFuture) {
        return toCompletableFuture(channelFuture, ChannelFuture::channel, CompletableFuture::new);
    }
}
