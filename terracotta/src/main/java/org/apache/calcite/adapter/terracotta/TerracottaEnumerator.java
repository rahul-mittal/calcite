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

import org.apache.calcite.linq4j.Enumerator;

import java.util.Iterator;

/**
 * Terracotta Enumerator.
 * @param <E> Type
 */
public abstract class TerracottaEnumerator<E> implements Enumerator<E> {

  private final Iterator iterator;
  private E current = null;

  protected TerracottaEnumerator(Iterator iterator) {
    this.iterator = iterator;
  }

  @Override public E current() {
    return current;
  }

  @Override public boolean moveNext() {
    if (iterator.hasNext()) {
      current = convert(iterator.next());
      return true;
    }
    current = null;
    return false;
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    // do nothing
  }

  public abstract E convert(Object obj);
}
