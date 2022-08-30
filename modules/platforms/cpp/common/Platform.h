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

#pragma once

/**
 * Macro SWITCH_WIN_OTHER that uses first option on Windows and second on any other OS.
 */
#ifdef WIN32
#   define SWITCH_WIN_OTHER(x, y) (x)
#else
#   define SWITCH_WIN_OTHER(x, y) (y)
#endif

#define LITTLE_ENDIAN 1
#define BIG_ENDIAN 2

#ifdef __BYTE_ORDER__
#   if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#       define BYTE_ORDER LITTLE_ENDIAN
#   else
#       define BYTE_ORDER BIG_ENDIAN
#   endif
#else
//TODO: Fix this
#   define BYTE_ORDER LITTLE_ENDIAN
#endif

