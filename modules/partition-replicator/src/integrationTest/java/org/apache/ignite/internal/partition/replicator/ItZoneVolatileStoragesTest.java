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

package org.apache.ignite.internal.partition.replicator;

import org.junit.jupiter.api.Test;

/**
 * Test class for scenarios that involves volatile storages.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
public class ItZoneVolatileStoragesTest extends ItAbstractColocationTest {

    /*
    У нас есть 3 типа хранилищ:
    1. Рафт-хранилище
    2. Хранилище состояний транзакций
    3. Хранилище данных таблиц

    На данный момент 1 и 3 могут быть волатильными:
    1. ReplicaManager#groupOptionsForPartition
    3. StorageEngine#createMvTable

    2 всегда персистентен (TxStateRocksDbStorage#getOrCreatePartitionStorage).

    Текущие вопросы:
    - Как мы будем определять волатильность рафт-хранилища если у нас хранилище состояний транзакций всё-равно персистентное.
    - Если будем

    Возможные сценарии:
    1.
     */

    @Test
    public void test1() {
        /*
        Мы хотим
         */
    }

}
