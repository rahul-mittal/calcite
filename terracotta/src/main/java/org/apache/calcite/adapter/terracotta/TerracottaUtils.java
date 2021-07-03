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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Terracotta Utils.
 */
public class TerracottaUtils {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(TerracottaUtils.class.getName());
  public static final String KEY_CELL_DEFINITION_NAME = "KEY";
  private TerracottaUtils() {
  }

  public static <K extends Comparable<K>> Object[] convertToRowValues(
      List<CellDefinition<?>> fields, Record<K> record) {
    Object[] values = new Object[fields.size()];
    values[0] = record.getKey();
    for (int i = 1; i < fields.size(); i++) {
      Optional<?> opt = record.get(fields.get(i));
      if (!opt.isPresent()) {
        values[i] = null;
      } else {
        values[i] = opt.get();
      }
    }

    return values;
  }

  public static <K extends Comparable<K>> RelDataType deduceRowType(JavaTypeFactory typeFactory,
      TerracottaScannableTable<K> terracottaScannableTable, List<CellDefinition<?>> fieldTypes) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    Dataset<K> dataset = terracottaScannableTable.getDataset();
    Optional<Record<K>> optionalRecord = dataset.reader().records().findAny();
    if (!optionalRecord.isPresent()) {
      throw new RuntimeException("No records in the dataset to deduce the schema");
    }

    // add key as a field
    names.add(KEY_CELL_DEFINITION_NAME);
    types.add(relDataType(terracottaScannableTable.getType(), typeFactory));
    if (fieldTypes != null) {
      fieldTypes.add(
          CellDefinition.define(KEY_CELL_DEFINITION_NAME,
              terracottaScannableTable.getType()));
    }

    optionalRecord.get().stream().forEach(cell -> {
      names.add(cell.definition().name());
      types.add(relDataType(cell.type(), typeFactory));
      if (fieldTypes != null) {
        fieldTypes.add(cell.definition());
      }
    });

    return typeFactory.createStructType(Pair.zip(names, types));
  }

  public static RelDataType relDataType(Type<?> type, JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(type.getJDKType());
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }
}
