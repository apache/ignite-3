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

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Map;
import org.apache.ignite.internal.rest.ErrorResult;
import org.junit.jupiter.api.Test;

/**
 * Testing the {@link RestApiHttpResponse}.
 */
public class RestApiHttpResponseTest {
    @Test
    void testToJson() {
        RestApiHttpResponse res = new RestApiHttpResponse(new DefaultHttpResponse(HttpVersion.HTTP_1_1, OK));

        Map<?, String> value = Map.of(
                "{root:{foo:foo,subCfg:{bar:bar}}}", "\"{root:{foo:foo,subCfg:{bar:bar}}}\"",
                Map.of("err", new ErrorResult("t", "m")), "{\"err\":{\"type\":\"t\",\"message\":\"m\"}}"
        );

        for (Map.Entry<?, String> e : value.entrySet()) {
            res.headers().clear();

            String act = new String(res.json(e.getKey()).content(), UTF_8);

            assertEquals(e.getValue(), act);
            assertEquals(res.headers().get(CONTENT_TYPE), APPLICATION_JSON.toString());
        }
    }
}
