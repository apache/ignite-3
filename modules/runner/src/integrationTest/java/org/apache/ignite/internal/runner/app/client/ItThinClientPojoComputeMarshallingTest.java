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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.runner.app.Jobs.PojoArg;
import org.apache.ignite.internal.runner.app.Jobs.PojoJob;
import org.apache.ignite.internal.runner.app.Jobs.PojoResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test the OOTB support for POJOs in compute api.
 */
@SuppressWarnings("resource")
public class ItThinClientPojoComputeMarshallingTest extends ItAbstractThinClientTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void pojoJobWithMarshallers(int targetNodeIdx) {
        // Given target node.
        var targetNode = node(targetNodeIdx);

        // When run job with custom marshaller for pojo argument and result.
        PojoResult result = client().compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(PojoJob.class).build(),
                new PojoArg().setIntValue(2).setStrValue("1")
        );

        // Then the job returns the expected result.
        assertThat(result.getLongValue(), is(3L));
    }
}
