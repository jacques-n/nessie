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

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;

import static org.apache.spark.sql.functions.*;

/**
 * Given a particular ref, generate all the referenced L2s from the ref.
 *
 * This loads the ref, figures out the last persisted L1, then recursively iterates through the L1s
 * parents until we find that L1s are no longer valid. For each valid L1, we output all the L1s.
 */
public class RefToL2Producer {

  public static BinaryBloomFilter getL2BloomFilter(SparkSession spark, Supplier<Store> store, GcPolicy policy, long size) throws AnalysisException {

    final Dataset<Row> refs = new ValueRetriever<InternalRef>(store, ValueType.REF).get(
        RefFrame.class,
        spark,
        Optional.of(r -> policy.isReferencedL1(r.getName(), r.getDt())),
        r -> {
          if (r.getType() == Type.BRANCH) {
            return RefFrame.of(r.getName(), r.getBranch().getLastDefinedParent());
          }
          return RefFrame.of(r.getName(), r.getTag().getCommit());
        }).select("name", "id");

    {
      // expose a table of all l1s.
      new ValueRetriever<L1>(store, ValueType.L1).get(L1Frame.class, spark, Optional.empty(), L1_CONVERTER)
        .createTempView("l1");
    }

    // create a potential view for l1s to consider.
    refs.select("id", "name").createOrReplaceTempView("potential");
    Dataset<Row> referencedL1s = null; // schema is id + children.

    // iterate over child -> parent relationships until we get to the root of each valid ref.
    while(true) {

      // the potential items joined with the available l1s.
      final Dataset<Row> joined = spark.sql("SELECT potential.name, l1.id, l1.parents, l1.children FROM potential JOIN l1 ON potential.id = l1.id");
      joined.createOrReplaceTempView("joined");

      if (referencedL1s == null) {
        // append direct references to the list of valid l1s (the rest of the algorithm below is focused on parents of the current reference).
        referencedL1s = joined.select("id", "children");
      }

      // for each l1, find all of the parents of that l1 and filter them based on the gc policy.
      final Dataset<Row> exploded = spark.sql(
          "SELECT name, exploded.parents.id AS id, exploded.parents.recurse AS recurse, l1.dt, l1.children FROM "
          + "(SELECT name, explode(parents) AS parents FROM joined as joined)exploded "
          + "JOIN l1 ON exploded.parents.id = l1.id")
          .filter(gc(policy));


      // if we didn't find any more children, terminate the loop.
      if (exploded.count() == 0) {
        break;
      }

      // append the additional l1s to the existing list of referenced l1s.
      referencedL1s = referencedL1s.unionAll(exploded.select("id", "children"));

      // replace the first potential list with this new potential list so we can loop and recurse
      // Important, the loop variable here is effectively a catalog view name.
      exploded.filter("recurse = true").select("id", "name").createOrReplaceTempView("potential");

      // TODO: put a break if loops too many times.
      // The maximum possible loop count should be longest commit history divided by ParentList.MAX_PARENT_LIST_SIZE.
    }

    // build a bloomfilter of all referenced l2's by exploding the tree of each l1's children.
    final Dataset<Row> ids = referencedL1s
        .withColumn("children", explode(col("children")))
        .withColumn("id", col("children.id"));
    return BinaryBloomFilter.aggregate(ids, "id");
 }

  static Function<L1, L1Frame> L1_CONVERTER = l1 -> {
    return new L1Frame(l1.getId(), l1.getDt(), l1.getParents().filter(id -> !id.equals(L1.EMPTY_ID)).collect(Collectors.toList()), l1.getL2References().collect(Collectors.toList()));
  };

  static FilterFunction<Row> gc(GcPolicy policy) {
    return r -> {
      String name = r.getAs("name");
      Long dt = r.getAs("dt");
      return policy.isReferencedL1(name, dt);
    };
  }

}

