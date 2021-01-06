/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.versioned;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;

public class ValueRetriever<T> {

  private final Supplier<Store> supplier;
  private final ValueType valueType;

  public ValueRetriever(Supplier<Store> supplier, ValueType valueType) {
    super();
    this.supplier = supplier;
    this.valueType = valueType;
  }

  @SuppressWarnings("unchecked")
  private Stream<T> stream() {
    return (Stream<T>) supplier.get().getValues(Object.class, valueType);
  }

  public <OUT> Dataset<OUT> get(Class<OUT> outClazz, SparkSession spark, Optional<Predicate<T>> filter, Function<T, OUT> converter) {
    List<OUT> out = this.stream()
        .filter(filter.orElse(t -> true))
        .map(converter)
        .collect(Collectors.toList());
    return spark.createDataset(out , Encoders.bean(outClazz));
  }

}