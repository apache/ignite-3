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

package org.apache.ignite.internal.cli.core.call;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.startsWith;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.ignite.internal.cli.core.exception.TestExceptionHandler;
import org.junit.jupiter.api.Test;

class PipelineTest {

    @Test
    void verboseTest() {
        // Given
        StringWriter out = new StringWriter();
        StringWriter errOut = new StringWriter();

        // When start pipeline with verbose
        CallExecutionPipeline.builder(new ThrowingStrCall())
                .inputProvider(StringCallInput::new)
                .exceptionHandler(new TestExceptionHandler())
                .output(new PrintWriter(out))
                .errOutput(new PrintWriter(errOut))
                .verbose(true)
                .build().runPipeline();

        // Then error output starts with the message from exception and contains verbose output
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), startsWith("Ooops!" + System.lineSeparator()));
        assertThat(errOut.toString(), containsString("verbose output"));
    }
}
