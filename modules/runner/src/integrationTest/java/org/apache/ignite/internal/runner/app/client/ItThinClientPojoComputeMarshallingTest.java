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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobDescriptor.Builder;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.internal.runner.app.Jobs.MapReducePojo;
import org.apache.ignite.internal.runner.app.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.Jobs.PojoJob;
import org.apache.ignite.internal.runner.app.Jobs.PojoResult;
import org.apache.ignite.internal.runner.app.Jobs.TwoStringPojo;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test the OOTB support for POJOs in compute api.
 */
@SuppressWarnings({"resource", "ThrowableNotThrown"})
public class ItThinClientPojoComputeMarshallingTest extends ItAbstractThinClientTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void pojoJob(int targetNodeIdx) {
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with provided pojo result class.
        PojoResult result = client().compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class).resultClass(PojoResult.class).build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertThat(result.getLongValue(), is(3L));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void pojoJobWithDifferentClass(int targetNodeIdx) {
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with pojo result class which is different from the actual class in the job.
        Builder<PojoArg, PojoResult1> builder = JobDescriptor.builder(PojoJob.class.getName());
        PojoResult1 result = client().compute().execute(
                JobTarget.node(targetNode),
                builder.resultClass(PojoResult1.class).build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertThat(result.longValue, is(3L));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void pojoJobWithoutResultClass(int targetNodeIdx) {
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for pojo argument and result.
        assertThrows(
                UnmarshallingException.class,
                () -> client().compute().execute(
                        JobTarget.node(targetNode),
                        JobDescriptor.builder(PojoJob.class).build(),
                        new PojoArg().setIntValue(2).setStrValue("1")
                ),
                "Can not unpack object because the pojo class is not provided but the object was packed as pojo."
        );
    }

    /** Pojo with the same layout as {@link org.apache.ignite.internal.runner.app.Jobs.PojoResult}. */
    public static class PojoResult1 {
        public long longValue;
    }

    @Test
    void mapReduce() {
        // When.
        TwoStringPojo result = client().compute().executeMapReduce(
                TaskDescriptor.builder(MapReducePojo.class)
                        .reduceJobResultClass(TwoStringPojo.class)
                        .build(),
                // input_O goes to 0 node and input_1 goes to 1 node
                new TwoStringPojo("Input_0", "Input_1")
        );

        // Then.
        assertThat(result.string_0, containsString("Input_0:marshalledOnClient:unmarshalledOnServer:processedOnServer"));
        // And.
        assertThat(result.string_1, containsString("Input_1:marshalledOnClient:unmarshalledOnServer:processedOnServer"));
    }
}
