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

package org.apache.ignite.internal.sql.engine.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.junit.jupiter.api.Test;

/**
 * Ensures that copying {@link NodeLeftException} does not change the exception message.
 */
public class NodeLeftExceptionCopyTest {
    @Test
    public void exceptionCopyKeepsOriginalMessage() {
        String nodeName = "node-1";
        Throwable origin = new NodeLeftException(nodeName);
        Throwable copy = ExceptionUtils.copyExceptionWithCause(new CompletionException(origin));

        assertThat(origin.getMessage(), not(equalTo(nodeName)));
        assertThat(origin.getMessage(), equalTo(copy.getMessage()));
    }
}
