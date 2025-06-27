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

import java.io.PrintWriter;

/** Builder for {@link CallExecutionPipeline}. */
public interface CallExecutionPipelineBuilder<I extends CallInput, T> {
    /**
     * Sets output {@link PrintWriter}.
     *
     * @param output Output print writer.
     * @return Instance of the builder.
     */
    CallExecutionPipelineBuilder<I, T> output(PrintWriter output);

    /**
     * Sets error output {@link PrintWriter}.
     *
     * @param errOutput Error output print writer.
     * @return Instance of the builder.
     */
    CallExecutionPipelineBuilder<I, T> errOutput(PrintWriter errOutput);

    /**
     * Sets verbosity status.
     *
     * @param verbose Verbosity status.
     * @return Instance of the builder.
     */
    CallExecutionPipelineBuilder<I, T> verbose(boolean[] verbose);

    /**
     * Builds the pipeline.
     *
     * @return Constructed pipeline.
     */
    CallExecutionPipeline<I, T> build();
}
