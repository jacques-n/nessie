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

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import static org.apache.spark.sql.functions.*;

/**
 * Convert an item to valid version of it's children.
 *
 * Given a particular item and a bloomfilter listing valid ids, determine if the current item is valid.
 * If the current item is valid, generate all the referenced children ids.
 */
public class IdProducer {

  /**
   * Given a bloom filter and list of items that can produce addition Ids, get the next bloom filter.
   * @param <T>
   * @param idFilter
   * @param supplier
   * @param targetCount
   * @return
   */
  public static <T extends HasId & Iterable<Id>> BinaryBloomFilter getNextBloomFilter(
      SparkSession spark,
      BinaryBloomFilter idFilter,
      ValueRetriever<T> supplier,
      long targetCount) {

    Predicate<T> predicate = t -> idFilter.mightContain(t.getId().getValue().asReadOnlyByteBuffer());
    Dataset<IdCarrier> carriers = supplier.get(IdCarrier.class, spark, Optional.of(predicate), (Function<T, IdCarrier>) IdCarrier::of);

    return BinaryBloomFilter.aggregate(carriers.withColumn("id", explode(col("children"))), "id.id");

  }

  public static class IdCarrier implements Serializable {
    private static final long serialVersionUID = -1421784708484600778L;

    private List<IdFrame> children;

    public static <T extends HasId & Iterable<Id>> IdCarrier of(T obj) {
      IdCarrier id = new IdCarrier();
      id.setChildren(StreamSupport.stream(obj.spliterator(), false).map(IdFrame::of).collect(Collectors.toList()));
      return id;
    }

    public List<IdFrame> getChildren() {
      return children;
    }

    public void setChildren(List<IdFrame> children) {
      this.children = children;
    }


  }

}
