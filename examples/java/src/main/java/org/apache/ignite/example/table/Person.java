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

package org.apache.ignite.example.table;

/**
 * Represents a Person entity with database mapping.
 *
 * Database schema:
 * - id: int PRIMARY KEY
 * - city_id: int
 * - name: varchar
 * - age: int
 * - company: varchar
 */
public class Person {
    private int id;
    private String cityId;
    private String name;
    private int age;
    private String company;

    public Person() {}

    public Person(int id, String cityId, String name, int age, String company) {
        this.id = id;
        this.cityId = cityId;
        this.name = name;
        this.age = age;
        this.company = company;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getCityId() { return cityId; }
    public void setCityId(String cityId) { this.cityId = cityId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }

    public String getCompany() { return company; }
    public void setCompany(String company) { this.company = company; }
}
