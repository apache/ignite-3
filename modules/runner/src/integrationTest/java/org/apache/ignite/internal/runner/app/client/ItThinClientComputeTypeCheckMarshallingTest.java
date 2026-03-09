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
import static org.apache.ignite.internal.IgniteExceptionTestUtils.hasMessage;
import static org.apache.ignite.internal.IgniteExceptionTestUtils.traceableException;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

import java.util.concurrent.CompletableFuture;
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
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test for exceptions that are thrown when marshallers are defined in a wrong way or throw an exception.
 */
public class ItThinClientComputeTypeCheckMarshallingTest extends ItAbstractThinClientTest {
    @Test
    void argumentMarshallerDefinedOnlyInJob() {
        // When submit job with custom argument marshaller that is defined in job but
        // client JobDescriptor does not declare the argument marshaller.
        JobExecution<String> result = submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ArgMarshallingJob.class).build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(FAILED)));
        assertResultFailsWithErr(
                result, Compute.MARSHALLING_TYPE_MISMATCH_ERR,
                "ComputeJob.inputMarshaller is defined, but the JobDescriptor.argumentMarshaller is not defined."
        );
    }

    @Test
    void resultMarshallerDefinedOnlyInJob() {
        // When submit job with custom result marshaller that is defined in job but
        // client JobDescriptor does not declare the result marshaller.
        JobExecution<String> result = submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ResultMarshallingJob.class).build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(COMPLETED)));
        assertResultFailsWithErr(
                result, Compute.MARSHALLING_TYPE_MISMATCH_ERR,
                "ComputeJob.resultMarshaller is defined, but the JobDescriptor.resultMarshaller is not defined."
        );
    }

    @Test
    void argumentMarshallerDefinedOnlyInDescriptor() {
        // When submit job with custom argument marshaller that is defined in client JobDescriptor but
        // job class does not declare the input marshaller.
        JobExecution<String> result = submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ResultMarshallingJob.class).argumentMarshaller(ByteArrayMarshaller.create()).build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(FAILED)));
        assertResultFailsWithErr(
                result, Compute.MARSHALLING_TYPE_MISMATCH_ERR,
                "JobDescriptor.argumentMarshaller is defined, but the ComputeJob.inputMarshaller is not defined."
        );
    }

    @Test
    void resultMarshallerDefinedOnlyInDescriptor() {
        // When submit job with custom result marshaller that is defined in client JobDescriptor but
        // job class does not declare the result marshaller.
        JobExecution<String> result = submit(
                JobTarget.node(node(1)),
                JobDescriptor.builder(ArgMarshallingJob.class)
                        .argumentMarshaller(ByteArrayMarshaller.create())
                        .resultMarshaller(ByteArrayMarshaller.create())
                        .build(),
                "Input"
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(COMPLETED)));
        assertResultFailsWithErr(
                result, Compute.MARSHALLING_TYPE_MISMATCH_ERR,
                "JobDescriptor.resultMarshaller is defined, but the ComputeJob.resultMarshaller is not defined."
        );
    }

    @Test
    void argumentMarshallerDoesNotMatch() {
        // When submit job with custom marshaller that is defined in job but
        // client JobDescriptor does not declare the result marshaller.
        JobExecution<Integer> result = submit(
                JobTarget.node(node(1)),
                // The descriptor does not match actual job arguments.
                JobDescriptor.<Integer, Integer>builder(ArgumentTypeCheckingMarshallingJob.class.getName())
                        .argumentMarshaller(new IntegerMarshaller())
                        .build(),
                1
        );

        await().until(result::stateAsync, willBe(jobStateWithStatus(FAILED)));
        assertResultFailsWithErr(
                result, Compute.MARSHALLING_TYPE_MISMATCH_ERR,
                "Exception in user-defined marshaller",
                hasMessage(containsString("java.lang.RuntimeException: User defined error."))
        );
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

        // The job has completed successfully, but result was not unmarshalled correctly
        assertResultFailsWithErr(
                result, Compute.MARSHALLING_TYPE_MISMATCH_ERR,
                "Exception in user-defined marshaller",
                instanceOf(ClassCastException.class)
        );
    }

    static class ArgumentTypeCheckingMarshallingJob implements ComputeJob<String, String> {
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

                    throw new RuntimeException("User defined error.");
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

    private static void assertResultFailsWithErr(JobExecution<?> result, int errCode, String expectedMessage) {
        assertResultFailsWithErr(result, errCode, expectedMessage, null);
    }

    private static void assertResultFailsWithErr(
            JobExecution<?> result,
            int errCode,
            String expectedMessage,
            @Nullable Matcher<? extends Throwable> causeMatcher
    ) {
        assertThat(
                result.resultAsync(),
                willThrow(traceableException(ComputeException.class, errCode, expectedMessage).withCause(causeMatcher))
        );
    }
}
