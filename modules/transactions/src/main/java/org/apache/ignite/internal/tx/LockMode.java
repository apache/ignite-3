/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx;

/**
 * Lock mode.
 */
public enum LockMode {
    SHARED,
    EXCLUSIVE,
    INTENTION_SHARED,
    INTENTION_EXCLUSIVE,
    SHARED_AND_INTENTION_EXCLUSIVE;

    public boolean isCompatible(LockMode lockMode) {
        switch (this) {
            case INTENTION_SHARED:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case INTENTION_EXCLUSIVE:
                    case SHARED:
                    case SHARED_AND_INTENTION_EXCLUSIVE:
                        return true;
                    default:
                        return false;

                }
            case INTENTION_EXCLUSIVE:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case INTENTION_EXCLUSIVE:
                        return true;
                    default:
                        return false;

                }
            case SHARED:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case SHARED:
                        return true;
                    default:
                        return false;

                }
            case SHARED_AND_INTENTION_EXCLUSIVE:
                switch (lockMode) {
                    case INTENTION_SHARED:
                        return true;
                    default:
                        return false;

                }
            default:
                return false;
        }
    }

    public boolean allowReenter(LockMode lockMode) {
        switch (this) {
            case INTENTION_SHARED:
                switch (lockMode) {
                    case INTENTION_SHARED:
                        return true;
                    default:
                        return false;

                }
            case INTENTION_EXCLUSIVE:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case INTENTION_EXCLUSIVE:
                        return true;
                    default:
                        return false;

                }
            case SHARED:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case SHARED:
                        return true;
                    default:
                        return false;

                }
            case SHARED_AND_INTENTION_EXCLUSIVE:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case INTENTION_EXCLUSIVE:
                    case SHARED:
                    case SHARED_AND_INTENTION_EXCLUSIVE:
                        return true;
                    default:
                        return false;

                }
            case EXCLUSIVE:
                switch (lockMode) {
                    case INTENTION_SHARED:
                    case INTENTION_EXCLUSIVE:
                    case SHARED:
                    case SHARED_AND_INTENTION_EXCLUSIVE:
                    case EXCLUSIVE:
                        return true;
                    default:
                        return false;

                }
            default:
                return false;
        }
    }
}
