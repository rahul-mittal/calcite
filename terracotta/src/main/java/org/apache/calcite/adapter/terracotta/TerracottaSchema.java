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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.manager.DatasetManager;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;

/**
 * Terracotta Schema.
 */
public class TerracottaSchema extends AbstractSchema {

  private final DatasetManager datasetManager;

  public TerracottaSchema(String hostname, int port) {
    ArrayList<InetSocketAddress> list = new ArrayList<>();
    list.add(InetSocketAddress.createUnresolved(hostname, port));
    try {
      this.datasetManager = DatasetManager.clustered(list).build();
    } catch (StoreException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    try {
      for (Map.Entry<String, Type<?>> datasetEntry : datasetManager.listDatasets().entrySet()) {
        Table table = new TerracottaScannableTable(datasetManager, datasetEntry.getKey(),
            datasetEntry.getValue());
        builder.put(datasetEntry.getKey(), table);
      }
    } catch (StoreException e) {
      e.printStackTrace();
    }

    return builder.build();
  }
}
