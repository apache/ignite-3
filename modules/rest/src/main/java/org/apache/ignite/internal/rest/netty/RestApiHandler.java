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

package org.apache.ignite.internal.rest.netty;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.RestApiHttpResponse;
import org.apache.ignite.internal.rest.routes.Router;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Main handler of REST HTTP chain. It receives http request, process it by {@link Router} and produce http response.
 */
public class RestApiHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    /** Ignite logger. */
    private final IgniteLogger log = IgniteLogger.forClass(getClass());

    /** Requests' router. */
    private final Router router;

    /**
     * Creates a new instance of API handler.
     *
     * @param router Router.
     */
    public RestApiHandler(Router router) {
        this.router = router;
    }

    /** {@inheritDoc} */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        CompletableFuture<DefaultFullHttpResponse> responseFuture = router.route(request)
                .map(route -> {
                    var response = new RestApiHttpResponse(new DefaultHttpResponse(HttpVersion.HTTP_1_1, OK));

                    return route.handle(request, response)
                            .thenApply(resp -> {
                                ByteBuf content = resp.content() != null
                                        ? Unpooled.wrappedBuffer(resp.content())
                                        : new EmptyByteBuf(UnpooledByteBufAllocator.DEFAULT);

                                return new DefaultFullHttpResponse(
                                        resp.protocolVersion(),
                                        resp.status(),
                                        content,
                                        resp.headers(),
                                        EmptyHttpHeaders.INSTANCE
                                );
                            });
                })
                .orElseGet(() -> CompletableFuture.completedFuture(
                        new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.NOT_FOUND)
                ));

        responseFuture
                .whenCompleteAsync((response, e) -> {
                    if (e != null) {
                        exceptionCaught(ctx, e);

                        return;
                    }

                    response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());

                    boolean keepAlive = HttpUtil.isKeepAlive(request);

                    if (keepAlive) {
                        if (!request.protocolVersion().isKeepAliveDefault()) {
                            response.headers().set(CONNECTION, KEEP_ALIVE);
                        }
                    } else {
                        response.headers().set(CONNECTION, CLOSE);
                    }

                    ChannelFuture f = ctx.writeAndFlush(response);

                    if (!keepAlive) {
                        f.addListener(ChannelFutureListener.CLOSE);
                    }
                }, ctx.executor());
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Failed to process http request:", cause);
        var res = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
    }
}
