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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.manager.DatasetManager;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Create dataset and insert records.
 */
public class TerracottaInitialize {

  public static final StringCellDefinition NAME = CellDefinition.defineString("NAME");
  public static final IntCellDefinition DEPARTMENTID = CellDefinition.defineInt("DEPARTMENTID");
  public static final IntCellDefinition AGE = CellDefinition.defineInt("AGE");

  private TerracottaInitialize() {
  }

  public static void main(String[] args) throws Exception {
    ArrayList<InetSocketAddress> list = new ArrayList<>();
    list.add(InetSocketAddress.createUnresolved("127.0.0.1", 9411));
    try (DatasetManager datasetManager = DatasetManager.clustered(list).build()) {
      DatasetConfiguration datasetConfiguration =
          datasetManager.datasetConfiguration()
              .offheap("main")
              .build();
      datasetManager.destroyDataset("employee");
      datasetManager.newDataset("employee", Type.INT, datasetConfiguration);
      datasetManager.destroyDataset("department");
      datasetManager.newDataset("department", Type.INT, datasetConfiguration);
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", Type.INT)) {
        AsyncDatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader().async();
        PrimitiveIterator.OfInt ageIterator = new Random().ints(25, 75).iterator();
        PrimitiveIterator.OfInt deptIterator =
            new Random().ints(1, Department.values().length+1).iterator();
        for (int i = 1; i <= 100; i++) {
          datasetWriterReader.add(i,
              NAME.newCell("Charles:" + i),
              AGE.newCell(ageIterator.next()),
              DEPARTMENTID.newCell(deptIterator.next()));
        }

        System.out.println("Num records = " + datasetWriterReader.records().count().get());
      }

      try (Dataset<Integer> dataset = datasetManager.getDataset("department", Type.INT)) {
        AsyncDatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader().async();
        for (int i = 1; i <= Department.values().length; i++) {
          datasetWriterReader.add(i,
              NAME.newCell(Department.values()[i % (Department.values().length)].name()));
        }

        System.out.println("Num records = " + datasetWriterReader.records().count().get());
      }
    }
  }

  enum Department {
    ENGINEERING,
    HR,
    ADMIN,
    HOUSEKEEPING,
    FINANCE
  };
}
