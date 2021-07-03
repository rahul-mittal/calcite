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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.manager.DatasetManager;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.adapter.terracotta.TerracottaUtils.convertToRowValues;

/**
 * Terracotta scannable table.
 * @param <K> key type of Dataset
 */
public class TerracottaScannableTable<K extends Comparable<K>> extends AbstractTable
    implements ScannableTable {

  private final String name;
  private Type<K> type;
  private final Dataset<K> dataset;
  private RelDataType relDataType;
  private List<CellDefinition<?>> fields;

  public TerracottaScannableTable(DatasetManager datasetManager, String name, Type<K> type) {
    try {
      this.dataset = datasetManager.getDataset(name, type);
    } catch (StoreException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    this.name = name;
    this.type = type;
    this.relDataType = null;
    this.fields = null;
  }

  @Override public String toString() {
    return "TerracottaScannableTable";
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    JavaTypeFactory typeFactory = root.getTypeFactory();
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new TerracottaEnumerator<Object[]>(dataset.reader().records().iterator()) {
          @Override public Object[] convert(Object obj) {
            Object values = convertToRowValues(getFields(typeFactory), (Record<K>) obj);
            if (values instanceof Object[]) {
              return (Object[]) values;
            }
            return new Object[]{values};
          }
        };
      }
    };
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (relDataType == null) {
      fields = new ArrayList<>();
      relDataType = TerracottaUtils.deduceRowType(
          (JavaTypeFactory) typeFactory, this, fields);
    }

    return relDataType;
  }

  public List<CellDefinition<?>> getFields(RelDataTypeFactory typeFactory) {
    if (fields == null) {
      getRowType(typeFactory);
    }
    return fields;
  }

  public String getName() {
    return name;
  }

  public Type<K> getType() {
    return type;
  }

  public Dataset<K> getDataset() {
    return dataset;
  }
}
