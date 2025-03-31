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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.matches;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test to make sure JOIN ORDER optimization returns equivalent plan, i.e. plan returning equal result set.
 */
public class ItJoinOrderTest extends BaseSqlIntegrationTest {
    private static final int PRODUCTS = 5000;

    @BeforeAll
    public static void initSchema() {
        int users = 10000;
        int orders = 20000;
        int orderDetails = 100000;
        int categories = 100;
        int reviews = 50000;
        int discounts = 2000;
        int warehouses = 50;
        int shippings = 15000;

        //noinspection ConcatenationWithEmptyString
        sqlScript("" 
                + "CREATE TABLE Users (\n"
                + "    UserID INT PRIMARY KEY,\n"
                + "    UserName VARCHAR(100),\n"
                + "    UserEmail VARCHAR(100)\n"
                + ");" 
                + "" 
                + "CREATE TABLE Orders (\n"
                + "    OrderID INT PRIMARY KEY,\n"
                + "    UserID INT,\n"
                + "    OrderDate DATE,\n"
                + "    TotalAmount DECIMAL(10, 2)\n"
                + ");" 
                + "" 
                + "CREATE TABLE Products (\n"
                + "    ProductID INT PRIMARY KEY,\n"
                + "    ProductName VARCHAR(100),\n"
                + "    Price DECIMAL(10, 2)\n"
                + ");" 
                + "" 
                + "CREATE TABLE OrderDetails (\n"
                + "    OrderDetailID INT PRIMARY KEY,\n"
                + "    OrderID INT,\n"
                + "    ProductID INT,\n"
                + "    Quantity INT\n"
                + ");" 
                + "" 
                + "CREATE TABLE Categories (\n"
                + "    CategoryID INT PRIMARY KEY,\n"
                + "    CategoryName VARCHAR(100)\n"
                + ");" 
                + "" 
                + "CREATE TABLE ProductCategories (\n"
                + "    ProductCategoryID INT PRIMARY KEY,\n"
                + "    ProductID INT,\n"
                + "    CategoryID INT\n"
                + ");" 
                + "" 
                + "CREATE TABLE Shipping (\n"
                + "    ShippingID INT PRIMARY KEY,\n"
                + "    OrderID INT,\n"
                + "    ShippingDate DATE,\n"
                + "    ShippingAddress VARCHAR(255)\n"
                + ");" 
                + "" 
                + "CREATE TABLE Reviews (\n"
                + "    ReviewID INT PRIMARY KEY,\n"
                + "    ProductID INT,\n"
                + "    UserID INT,\n"
                + "    ReviewText VARCHAR,\n"
                + "    Rating INT\n"
                + ");" 
                + "" 
                + "CREATE TABLE Discounts (\n"
                + "    DiscountID INT PRIMARY KEY,\n"
                + "    ProductID INT,\n"
                + "    DiscountPercentage DECIMAL(5, 2),\n"
                + "    ValidUntil DATE\n"
                + ");" 
                + "" 
                + "CREATE TABLE Warehouses (\n"
                + "    WarehouseID INT PRIMARY KEY,\n"
                + "    WarehouseName VARCHAR(100),\n"
                + "    Location VARCHAR(100)\n"
                + ");"
        );

        sql("INSERT INTO Users SELECT x, 'User_' || x::VARCHAR, 'user' || x::VARCHAR || '@example.com' "
                + "FROM system_range(1, ?)", users);

        sql("INSERT INTO Orders SELECT x, 1 + RAND_INTEGER(?), date '2020-01-01' + RAND_INTEGER(365)::INTERVAL DAYS, "
                + "ROUND(50.0 + 1950.0 * RAND(), 2) FROM system_range(1, ?)", users - 1, orders);

        sql("INSERT INTO Products SELECT x, 'Product_' || x::VARCHAR, "
                + "ROUND(5.0 + 495.0 * RAND(), 2) FROM system_range(1, ?)", PRODUCTS);

        sql("INSERT INTO OrderDetails SELECT x, 1 + RAND_INTEGER(?), 1 + RAND_INTEGER(?), "
                + "1 + RAND_INTEGER(9) FROM system_range(1, ?)", orders - 1, PRODUCTS - 1, orderDetails);

        sql("INSERT INTO Categories SELECT x, 'Category_' || x::VARCHAR FROM system_range(1, ?)", categories);

        sql("INSERT INTO ProductCategories SELECT x, 1 + RAND_INTEGER(?),"
                + " 1 + RAND_INTEGER(?) FROM system_range(1, ?)", PRODUCTS - 1, categories - 1, PRODUCTS);

        sql("INSERT INTO Shipping SELECT x, 1 + RAND_INTEGER(?), date '2020-01-01' + RAND_INTEGER(365)::INTERVAL DAYS, "
                + " 'Address_' || x::VARCHAR FROM system_range(1, ?)", orders - 1, shippings);

        sql("INSERT INTO Reviews SELECT x, 1 + RAND_INTEGER(?), 1 + RAND_INTEGER(?)"
                        + ", 'This is a review for product ' || x::VARCHAR, 1 + RAND_INTEGER(4) FROM system_range(1, ?)",
                PRODUCTS - 1, users - 1, reviews);

        sql("INSERT INTO Discounts SELECT x, 1 + RAND_INTEGER(?), ROUND(5.0 + 45.0 * RAND(), 2) "
                        + ", date '2020-01-01' + RAND_INTEGER(365)::INTERVAL DAYS FROM system_range(1, ?)",
                PRODUCTS - 1, discounts);

        sql("INSERT INTO Warehouses SELECT x, 'Warehouse_' || x::VARCHAR, "
                + "'Location_' || x::VARCHAR FROM system_range(1, ?)", warehouses);

        gatherStatistics();
    }

    @ParameterizedTest
    @EnumSource(Query.class)
    void joinOrderOptimizationProvidesEquivalentPlan(Query query) {
        String originalText = query.text();
        String textWithEnforcedJoinOrder = originalText
                .replace("SELECT", "SELECT /*+ enforce_join_order */ ");

        Object[] params = query.params();

        List<List<Object>> expectedResult = sql(textWithEnforcedJoinOrder, params);

        Assumptions.assumeFalse(expectedResult.isEmpty());

        QueryChecker checker = assertQuery(originalText)
                .withParams(params);

        expectedResult.forEach(row -> checker.returns(row.toArray()));

        checker.check();
    }

    @ParameterizedTest
    @MethodSource("joinTypesWithRulesToDisable")
    @SuppressWarnings("ConcatenationWithEmptyString")
    void joinWithProjectionOnTopReturnsValidResults(JoinType joinType, List<String> rulesToDisable) {
        String queryToAcquireExpectedResults = format("" 
                + "SELECT p.*, d.* " 
                + "  FROM Products p " 
                + "    {} JOIN Discounts d ON p.ProductID = d.ProductID" 
                + "  JOIN Reviews r ON p.ProductID = r.ProductID" 
                + " WHERE r.Rating > 2", joinType
        );

        String queryToValidate = format(""
                + "SELECT /*+ enforce_join_order, disable_rule({}) */ p.*, d.* "
                + "  FROM Discounts d "
                + "    {} JOIN Products p ON p.ProductID = d.ProductID"
                + "  JOIN Reviews r ON p.ProductID = r.ProductID"
                + " WHERE r.Rating > 2",
                '\'' + String.join("', '", rulesToDisable) + '\'',
                joinType.swap()
        );

        List<List<Object>> expectedResult = sql(queryToAcquireExpectedResults);

        Assumptions.assumeFalse(expectedResult.isEmpty());

        QueryChecker checker = assertQuery(queryToValidate)
                .matches(matches(".*Project.*Join.*Join.*"));

        expectedResult.forEach(row -> checker.returns(row.toArray()));

        checker.check();
    }

    private static Stream<Arguments> joinTypesWithRulesToDisable() {
        List<String> joinConverterRules = List.of("HashJoinConverter", "MergeJoinConverter", "NestedLoopJoinConverter");

        List<Arguments> combinations = new ArrayList<>();

        for (JoinType joinType : JoinType.values()) {
            for (String joinAlgo : joinConverterRules) {
                List<String> rulesToDisable = joinConverterRules.stream()
                        .filter(ruleName -> !joinAlgo.equals(ruleName))
                        .collect(Collectors.toList());

                combinations.add(Arguments.of(joinType, rulesToDisable));
            }
        }

        return combinations.stream();
    }

    enum Query {
        ORDERS_WITH_TOTAL_REVENUE_AND_SHIPPING_DETAILS(
                "SELECT \n"
                        + "    O.OrderID, O.OrderDate, S.ShippingAddress, SUM(OD.Quantity * P.Price) AS TotalOrderValue\n"
                        + " FROM Orders O, Shipping S, OrderDetails OD, Products P\n"
                        + "WHERE O.OrderID = S.OrderID\n"
                        + "  AND O.OrderID = OD.OrderID\n"
                        + "  AND OD.ProductID = P.ProductID\n"
                        + "GROUP BY O.OrderID, O.OrderDate, S.ShippingAddress;"
        ),

        TOP_RATED_PRODUCTS_AND_THEIR_REVIEWERS(
                "SELECT \n"
                        + "    P.ProductName, R.Rating, U.UserName, R.ReviewText\n"
                        + " FROM Products P, Reviews R, Users U\n"
                        + "WHERE P.ProductID = R.ProductID\n"
                        + "  AND R.UserID = U.UserID\n"
                        + "  AND R.Rating IN (4, 5);"
        ),

        USER_ORDERS_WITH_PRODUCTS_IN_MULTIPLE_CATEGORIES(
                "SELECT \n"
                        + "    U.UserName, O.OrderID, COUNT(DISTINCT C.CategoryName) AS Categories\n"
                        + " FROM Users U, Orders O, OrderDetails OD, Products P, ProductCategories PC, Categories C\n"
                        + "WHERE U.UserID = O.UserID\n"
                        + "  AND O.OrderID = OD.OrderID\n"
                        + "  AND OD.ProductID = P.ProductID\n"
                        + "  AND P.ProductID = PC.ProductID\n"
                        + "  AND PC.CategoryID = C.CategoryID\n"
                        + "GROUP BY U.UserName, O.OrderID;"
        ),

        PRODUCTS_STORED_IN_WAREHOUSES_BY_CATEGORY(
                "SELECT \n"
                        + "    W.WarehouseName, C.CategoryName, P.ProductName\n"
                        + " FROM Warehouses W, Products P, ProductCategories PC, Categories C\n"
                        + "WHERE W.WarehouseID = (P.ProductID % 5 + 1)\n"
                        + "  AND P.ProductID = PC.ProductID\n"
                        + "  AND PC.CategoryID = C.CategoryID;"
        ),

        //CHECKSTYLE:OFF
        // For some reason checkstyle fails here with 'lambda arguments' has incorrect indentation level 16, expected level should be 32 
        USERS_WHO_HAVE_WRITTEN_REVIEWS_FOR_A_SPECIFIC_PRODUCT(
                "SELECT \n"
                        + "    U.UserName, P.ProductName, R.ReviewText, R.Rating\n"
                        + " FROM Users U, Reviews R, Products P\n"
                        + "WHERE U.UserID = R.UserID\n"
                        + "  AND R.ProductID = P.ProductID\n"
                        + "  AND P.ProductName = 'Product_' || ?::varchar;",
                () -> new Object[]{ThreadLocalRandom.current().nextInt(1, PRODUCTS)}
        ),
        //CHECKSTYLE:ON

        LIST_OF_PRODUCTS_WITH_DISCOUNTS_APPLIED_AND_THEIR_FINAL_PRICES(
                "SELECT \n"
                        + "    P.ProductName, P.Price, D.DiscountPercentage, \n"
                        + "    (P.Price * (1 - D.DiscountPercentage / 100)) AS FinalPrice\n"
                        + " FROM Products P, Discounts D\n"
                        + "WHERE P.ProductID = D.ProductID;"
        ),

        ORDERS_SHIPPED_WITH_TOTAL_QUANTITY_AND_SHIPPING_ADDRESS(
                "SELECT \n"
                        + "    O.OrderID, O.OrderDate, S.ShippingAddress, SUM(OD.Quantity) AS TotalQuantity\n"
                        + " FROM Orders O, Shipping S, OrderDetails OD\n"
                        + "WHERE O.OrderID = S.OrderID\n"
                        + "  AND O.OrderID = OD.OrderID\n"
                        + "GROUP BY O.OrderID, O.OrderDate, S.ShippingAddress;"
        ),

        AVERAGE_RATING_OF_PRODUCTS_IN_EACH_CATEGORY(
                "SELECT \n"
                        + "    C.CategoryName, P.ProductName, AVG(R.Rating) AS AvgRating\n"
                        + " FROM Categories C, ProductCategories PC, Products P, Reviews R\n"
                        + "WHERE C.CategoryID = PC.CategoryID\n"
                        + "  AND PC.ProductID = P.ProductID\n"
                        + "  AND P.ProductID = R.ProductID\n"
                        + "GROUP BY C.CategoryName, P.ProductName;"
        ),

        PRODUCTS_ORDERED_BY_EACH_USER(
                "SELECT \n"
                        + "    U.UserName, P.ProductName, SUM(OD.Quantity) AS TotalQuantity\n"
                        + " FROM Users U, Orders O, OrderDetails OD, Products P\n"
                        + "WHERE U.UserID = O.UserID\n"
                        + "  AND O.OrderID = OD.OrderID\n"
                        + "  AND OD.ProductID = P.ProductID\n"
                        + "GROUP BY U.UserName, P.ProductName;"
        ),

        TOTAL_REVENUE_GENERATED_BY_EACH_USER(
                "SELECT \n"
                        + "    U.UserID, U.UserName, SUM(O.TotalAmount) AS TotalRevenue\n"
                        + " FROM Users U, Orders O\n"
                        + "WHERE U.UserID = O.UserID\n"
                        + "GROUP BY U.UserID, U.UserName;"
        ),

        JOIN_ALL_TABLES(
                "SELECT \n"
                        + "    U.UserID, U.UserName, O.OrderID, O.OrderDate, P.ProductName, OD.Quantity, \n"
                        + "    C.CategoryName, S.ShippingAddress, R.Rating, D.DiscountPercentage, W.WarehouseName\n"
                        + " FROM Users U, Orders O, OrderDetails OD, Products P, ProductCategories PC, Categories C, \n"
                        + "    Shipping S, Reviews R, Discounts D, Warehouses W\n"
                        + "WHERE U.UserID = O.UserID\n"
                        + "  AND O.OrderID = OD.OrderID\n"
                        + "  AND OD.ProductID = P.ProductID\n"
                        + "  AND P.ProductID = PC.ProductID\n"
                        + "  AND PC.CategoryID = C.CategoryID\n"
                        + "  AND O.OrderID = S.OrderID\n"
                        + "  AND P.ProductID = R.ProductID"
                        + "  AND U.UserID = R.UserID\n"
                        + "  AND P.ProductID = D.ProductID\n"
                        + "  AND W.WarehouseID = (P.ProductID % 5 + 1);"
        );

        private final String text;
        private final Supplier<Object[]> paramsSupplier;

        Query(String text) {
            this(text, () -> ArrayUtils.OBJECT_EMPTY_ARRAY);
        }

        Query(String text, Supplier<Object[]> paramsSupplier) {
            this.text = text;
            this.paramsSupplier = paramsSupplier;
        }

        String text() {
            return text;
        }

        Object[] params() {
            return paramsSupplier.get();
        }
    }

    private enum JoinType {
        INNER, OUTER, LEFT, RIGHT;

        JoinType swap() {
            switch (this) {
                case LEFT: return RIGHT;
                case RIGHT: return LEFT;
                default: return this;
            }
        }

        @Override
        public String toString() {
            if (this == OUTER) {
                return "FULL OUTER";
            }

            return name();
        }
    }
}
