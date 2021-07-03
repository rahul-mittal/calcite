/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.terracotta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Terracotta JDBC example.
 */
public class TerracottaJDBCExample {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(TerracottaJDBCExample.class.getName());

  private TerracottaJDBCExample() {
  }

  public static void main(String[] args) throws Exception {
    Properties info = new Properties();
    final String model = "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  schemas: [\n"
        + "     {\n"
        + "       type: 'custom',\n"
        + "       name: 'TEST',\n"
        + "       factory: 'org.apache.calcite.adapter.terracotta"
        + ".TerracottaSchemaFactory',\n"
        + "       operand: {\n"
        + "         hostName: 'localhost',\n"
        + "         hostPort: '9411'\n"
        + "       }\n"
        + "     }\n"
        + "  ]\n"
        + "}";
    info.put("model", model);

    Class.forName("org.apache.calcite.jdbc.Driver");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    Statement statement = connection.createStatement();

    /*ResultSet resultSet =
        statement.executeQuery(
            "SELECT * " +
                "FROM " +
                "\"TEST\".\"employee\"");*/

    /*ResultSet resultSet =
        statement.executeQuery(
        "SELECT EMP.KEY, EMP.NAME AS EMP_NAME, DEPT.NAME AS DEPT_NAME " +
            "FROM " +
            "\"TEST\".\"employee\" AS EMP,  " +
            "\"TEST\".\"department\" as DEPT " +
            "WHERE " +
            "EMP.DEPARTMENTID = DEPT.KEY " +
            "ORDER BY " +
            "EMP.KEY");*/

    /*ResultSet resultSet =
        statement.executeQuery(
        "SELECT EMP.KEY, EMP.NAME AS EMP_NAME, DEPT.NAME AS DEPT_NAME " +
            "FROM " +
            "\"TEST\".\"employee\" AS EMP  " +
            "INNER JOIN " +
            "\"TEST\".\"department\" as DEPT " +
            "ON " +
            "EMP.DEPARTMENTID = DEPT.KEY " +
            "ORDER BY " +
            "EMP.KEY");*/

    /*ResultSet resultSet =
        statement.executeQuery(
        "SELECT DEPT.NAME, COUNT(*) AS NUM_EMPLOYEES " +
            "FROM " +
              "\"TEST\".\"employee\" AS EMP,  " +
              "\"TEST\".\"department\" as DEPT " +
            "WHERE " +
              "EMP.DEPARTMENTID = DEPT.KEY AND EMP.AGE > 50" +
            "GROUP BY " +
              "DEPT.NAME " +
            "HAVING " +
              "DEPT.NAME IN ('HR', 'ENGINEERING')");*/

    ResultSet resultSet =
        statement.executeQuery(
        "SELECT DEPT.NAME, COUNT(*) AS NUM_EMPLOYEES " +
            "FROM " +
              "\"TEST\".\"employee\" AS EMP " +
                      "INNER JOIN " +
              "\"TEST\".\"department\" as DEPT " +
                  "ON EMP.DEPARTMENTID = DEPT.KEY " +
            "WHERE EMP.AGE > 50" +
            "GROUP BY " +
              "DEPT.NAME " +
            "HAVING " +
              "DEPT.NAME IN ('HR', 'ENGINEERING')");

    final StringBuilder buf = new StringBuilder();

    while (resultSet.next()) {

      int columnCount = resultSet.getMetaData().getColumnCount();

      for (int i = 1; i <= columnCount; i++) {

        buf.append(i > 1 ? "; " : "")
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
      }

      LOGGER.info("Entry: " + buf.toString());

      buf.setLength(0);
    }

    resultSet.close();
    statement.close();
    connection.close();

  }
}
