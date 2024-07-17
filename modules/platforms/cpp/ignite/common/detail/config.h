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

#pragma once

#ifndef __has_include
# define __has_include(x) 0
#endif

#if __has_include(<version>)
# include <version>
#endif

#ifndef __has_attribute
# define __has_attribute(x) 0
#endif

#if defined(_WIN32)
# define IGNITE_CALL __stdcall
# define IGNITE_EXPORT __declspec(dllexport)
# define IGNITE_IMPORT __declspec(dllimport)
#else
# define IGNITE_CALL
# if __has_attribute(visibility) || (defined(__GNUC__) && ((__GNUC__ > 4) || (__GNUC__ == 4) && (__GNUC_MINOR__ > 2)))
#  define IGNITE_EXPORT __attribute__((visibility("default")))
#  define IGNITE_IMPORT __attribute__((visibility("default")))
# else
#  define IGNITE_EXPORT
#  define IGNITE_IMPORT
# endif
#endif

#ifdef IGNITE_IMPL
# define IGNITE_API IGNITE_EXPORT
#else
# define IGNITE_API IGNITE_IMPORT
#endif

/**
 * Macro IGNITE_SWITCH_WIN_OTHER that uses first option on Windows and second on any other OS.
 */
#ifdef WIN32
# define IGNITE_SWITCH_WIN_OTHER(x, y) x
#else
# define IGNITE_SWITCH_WIN_OTHER(x, y) y
#endif

#ifndef UNUSED_VALUE
# define UNUSED_VALUE (void)
#endif // UNUSED_VALUE
