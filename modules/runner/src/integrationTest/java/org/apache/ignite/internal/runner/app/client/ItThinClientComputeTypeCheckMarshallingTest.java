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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.runner.app.Jobs.ArgMarshallingJob;
import org.apache.ignite.internal.runner.app.Jobs.ResultMarshallingJob;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.marshalling.ByteArrayMarshaller;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnsupportedObjectTypeMarshallingException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test for exceptions that are thrown when marshallers are defined in a wrong way or throw an exception.
 */
@SuppressWarnings("resource")
public class ItThinClientComputeTypeCheckMarshallingTest extends ItAbstractThinClientTest {
    @Test
    void argumentMarshallerDefinedOnlyInJob() {
        // When submit job with custom marshaller that is defined in job but
        // client JobDescriptor does not declare the argument marshaller.
        JobExecution<String> result = submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ArgMarshallingJob.class).build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(FAILED)));
        assertResultFailsWithErr(Compute.MARSHALLING_TYPE_MISMATCH_ERR, result);
    }

    @Test
    void resultMarshallerDefinedOnlyInJob() {
        // When submit job with custom marshaller that is defined in job but
        // client JobDescriptor does not declare the result marshaller.
        JobExecution<String> result = submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ResultMarshallingJob.class).build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(COMPLETED)));
        assertThat(result.resultAsync(), willThrow(ComputeException.class));
    }

    @Test
    void argumentMarshallerDoesNotMatch() {
        // When submit job with custom marshaller that is defined in job but
        // client JobDescriptor does not declare the result marshaller.
        JobExecution<Integer> result = submit(
                JobTarget.node(node(1)),
                // The descriptor does not match actual job arguments.
                JobDescriptor.<Integer, Integer>builder(ArgumentTypeCheckingmarshallingJob.class.getName())
                        .argumentMarshaller(new IntegerMarshaller())
                        .build(),
                1
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(FAILED)));
        assertResultFailsWithErr(Compute.MARSHALLING_TYPE_MISMATCH_ERR, result);
    }

    @Test
    void resultMarshallerDoesNotMatch() {
        // When submit job with custom marshaller that is defined in job but the client JobDescriptor
        // declares the result marshaller which is not compatible with the marshaller in the job.
        JobExecution<Integer> result = submit(
                JobTarget.node(node(1)),
                // The descriptor does not match actual result.
                JobDescriptor.<String, Integer>builder(ResultMarshallingJob.class.getName())
                        .resultMarshaller(new IntegerMarshaller())
                        .build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(COMPLETED)));

        // The job has completed successfully, but result was not unmarshaled
        assertThrowsWithCause(() -> result.resultAsync().join(), ClassCastException.class);
    }

    static class ArgumentTypeCheckingmarshallingJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable String arg) {
            return completedFuture(arg);
        }

        @Override
        public Marshaller<String, byte[]> inputMarshaller() {
            return new ByteArrayMarshaller<>() {
                @Override
                public @Nullable String unmarshal(byte @Nullable [] raw) {
                    Object obj = ByteArrayMarshaller.super.unmarshal(raw);
                    if (obj == null) {
                        return null;
                    }

                    if (obj instanceof String) {
                        return (String) obj;
                    }

                    throw new UnsupportedObjectTypeMarshallingException(obj.getClass());
                }
            };
        }
    }

    private static class IntegerMarshaller implements Marshaller<Integer, byte[]> {

        @Override
        public byte @Nullable [] marshal(@Nullable Integer object) throws UnsupportedObjectTypeMarshallingException {
            return ByteArrayMarshaller.create().marshal(object);
        }

        @Override
        public @Nullable Integer unmarshal(byte @Nullable [] raw) throws UnsupportedObjectTypeMarshallingException {
            return ByteArrayMarshaller.<Integer>create().unmarshal(raw);
        }
    }

    private static void assertResultFailsWithErr(int errCode, JobExecution<?> result) {
        var ex = assertThrows(CompletionException.class, () -> result.resultAsync().join());
        assertThat(ex.getCause(), instanceOf(ComputeException.class));
        assertThat(((ComputeException) ex.getCause()).code(), equalTo(errCode));
    }
}
