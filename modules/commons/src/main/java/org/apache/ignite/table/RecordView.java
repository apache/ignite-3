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

package org.apache.ignite.table;

import java.util.Collection;

/**
 * Record adapter for Table.
 */
public interface RecordView<T> {
    /**
     * Fills given record with the values from the table.
     *
     * @param keyRec Record with key fields set.
     * @return Record with all fields filled from the table.
     */
    public T get(T objToFill);
    public Collection<T> select(Template template);


    public <K> boolean remove(K row); // Remove by key
    public boolean removeExact(T row);// Remove exact value

    public void remove(Template template);


    class Template{
    String[] fld;
    Object[] obj;

}

    class Person {
        int keyFld1;
        int keyFld2;

        String name;
    }
    /**
     * Fills given records with the values from the table.
     *
     * @param keyRecs Records with key fields set.
     * @return Records with all fields filled from the table.
     */
    public Collection<T> getAll(Collection<T> keyRecs);

    /**
     * Inserts new record into the table if it is not exists or replace existed one.
     *
     * @param rec Record to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean upsert(T rec);

    /**
     * Inserts new record into the table if it is not exists or replace existed one.
     *
     * @param recs Records to be inserted into table.
     */
    public void putAll(Collection<T> recs);

    /**
     * Inserts record into the table if not exists.
     *
     * @param rec Record to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean insert(T rec);

    /**
     * Insert record into the table and return previous record.
     *
     * @param rec Record to be inserted into table.
     * @return Record that was replaced, {@code null} otherwise.
     */
    public T getAndUpsert(T rec);

    /**
     * Replaces an existed record in the table with the given one.
     *
     * @param rec Record to replace with.
     * @return {@code True} if the record replaced successfully, {@code false} otherwise.
     */
    public boolean replace(T rec);

    /**
     * Replaces an expected record in the table with the new one.
     *
     * @param oldRec Record to be replaced in table.
     * @param newRec Record to replace with.
     * @return {@code True} if the record replaced successfully, {@code false} otherwise.
     */
    public boolean replace(T oldRec, T newRec);

    /**
     * Replaces an existed record in the table and return the replaced one.
     *
     * @param rec Record to be inserted into table.
     * @return Record that was replaced with given one, {@code null} otherwise.
     */
    public T getAndReplace(T rec);

    /**
     * Remove record from table.
     *
     * @param keyRec Record with key fields set.
     * @return {@code True} if record was successfully removed, {@code  false} otherwise.
     */
    public boolean delete(T keyRec);
    public boolean delete(T template, Criteria c); //TODO: Hibernate CriteriaAPI, + Hazelcast

    /**
     * Remove exact record from table.
     *
     * @param oldRec Record to be removed.
     * @return {@code True} if record was successfully removed, {@code  false} otherwise.
     */
    // TODO: Do we need a separate method like this, which semantically equals to 'replace(oldRec, null)'
    // If no, then how to deal with empty value columns and non-null column defaults?
    public boolean removeExact(T oldRec);

    /**
     * Remove record from table.
     *
     * @param rec Record with key fields set.
     * @return Record that was removed, {@code null} otherwise.
     */
    public T getAndRemove(T rec);

    // TODO: Do we need a separate method 'getAndRemoveExact(T rec)', which semantically equals to 'getAndReplace(oldRed, null)'?

    /**
     * Remove records from table.
     *
     * @param recs Records with key fields set.
     */
    public void removeAll(Collection<T> recs);
}
