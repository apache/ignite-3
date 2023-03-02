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

package org.apache.ignite.internal.rest.authentication;

import io.micronaut.security.authentication.AuthenticationResponse;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Implementation of {@link Subscriber} for tests. */
public class TestAuthenticationSubscriber implements Subscriber<AuthenticationResponse> {

    @Nullable
    private volatile AuthenticationResponse lastResponse;

    @Nullable
    private volatile Throwable lastError;

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(1);
    }

    @Override
    public void onNext(AuthenticationResponse authenticationResponse) {
        lastError = null;
        lastResponse = authenticationResponse;
    }

    @Override
    public void onError(Throwable throwable) {
        lastResponse = null;
        lastError = throwable;
    }

    @Override
    public void onComplete() {

    }

    @Nullable
    public AuthenticationResponse lastResponse() {
        return lastResponse;
    }

    @Nullable
    public Throwable lastError() {
        return lastError;
    }
}
