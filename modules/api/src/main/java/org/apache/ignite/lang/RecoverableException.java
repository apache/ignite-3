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

package org.apache.ignite.lang;

/**
 * This is a marker interface to indicate a recoverable error.
 * When an exception marked by {@link RecoverableException} is thrown, it means that an operation failed for some reason,
 * but this failure is not fatal and the operation can be retried.
 *
 * <code>
 *     try {
 *         Transaction tx = ignite.transactions().begin();
 *         ...
 *         tx.commit();
 *     } catch (TransactionException t) {
 *         if (t instanceof RecoverableException) {
 *             // put your retry logic here.
 *         }
 *     }
 * </code>
 */
public interface RecoverableException {
}
