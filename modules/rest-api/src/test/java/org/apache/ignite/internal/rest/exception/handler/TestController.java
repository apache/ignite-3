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

package org.apache.ignite.internal.rest.exception.handler;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import java.util.List;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Test controller.
 */
@Controller("/test")
public class TestController {
    private final ThrowableProvider throwableProvider;

    public TestController(ThrowableProvider throwableProvider) {
        this.throwableProvider = throwableProvider;
    }

    @Get("/throw-exception")
    public String throwException() throws Throwable {
        throw throwableProvider.throwable();
    }

    @Get("/list")
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public List<EchoMessage> list(@QueryValue @Min(0) int greatThan, @QueryValue(defaultValue = "10") @Max(10) int lessThan) {
        return List.of();
    }

    @Get(value = "/list/{id}", produces = "application/json")
    public int get(@PathVariable int id) {
        return id;
    }

    @Post(value = "/echo", consumes = "application/json")
    public EchoMessage echo(@Body EchoMessage dto) {
        return dto;
    }

    @Get("/sleep")
    public void sleep(@QueryValue(defaultValue = "1000") long millis) throws InterruptedException {
        Thread.sleep(millis);
    }
}
