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
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.ValueWorker;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import scala.Function1;

/**
 * Operation which identifies unreferenced assets.
 */
public class IdentifyUnreferencedAssets<T extends HasId> {

  private final StoreWorker<T, ?> storeWorker;
  private final Supplier<Store> store;
  private final SparkSession spark;
  private final GcOptions options;

  public IdentifyUnreferencedAssets(
      StoreWorker<T, ?> storeWorker,
      Supplier<Store> store,
      SparkSession spark,
      GcOptions options) {
    super();
    this.storeWorker = storeWorker;
    this.store = store;
    this.spark = spark;
    this.options = options;
  }

//  public static void main(String[] args) {
//    PipelineOptionsFactory.register(GcOptions.class);
//    GcOptions options = PipelineOptionsFactory.fromArgs(args).as(GcOptions.class);
//    Pipeline p = Pipeline.create(options);
//    IdentifyUnreferencedAssets.go(null, null, p, options);
//  }

  public Dataset<UnreferencedItem> identify() throws AnalysisException{
    return go(storeWorker, store, spark, options);
  }

  private static <T> Dataset<UnreferencedItem> go(StoreWorker<T, ?> storeWorker, Supplier<Store> store, SparkSession spark, GcOptions options) throws AnalysisException {

    final ValueWorker<T> valueWorker = storeWorker.getValueWorker();

    // get bloomfilter of valid l2s (based on the given gc policy).
    final BinaryBloomFilter l2BloomFilter = RefToL2Producer.getL2BloomFilter(spark, store, GcPolicy.of(options), options.getBloomFilterCapacity());

    // get bloomfilter of valid l3s.
    final BinaryBloomFilter l3BloomFilter = IdProducer.getNextBloomFilter(spark, l2BloomFilter, new ValueRetriever<L2>(store, ValueType.L2), options.getBloomFilterCapacity());

    // get a bloom filter of all values that are referenced by a valid value.
    final BinaryBloomFilter validValueIds = IdProducer.getNextBloomFilter(spark, l3BloomFilter, new ValueRetriever<L3>(store, ValueType.L2), options.getBloomFilterCapacity());

    // get all values.
    final Dataset<ByteFrame> values = new ValueRetriever<InternalValue>(store, ValueType.VALUE).get(ByteFrame.class, spark, Optional.empty(), new ConvertValues());

    // for each value, determine if it is a valid value. If it is, generate a referenced asset. If not, generate a non-referenced asset.
    // this is a single output that has a categorization column
    Dataset<CategorizedAssetKey> assets = values.flatMap(new AssetCategorizer<T>(validValueIds, storeWorker.getValueWorker()), Encoders.bean(CategorizedAssetKey.class));

    // generate a bloom filter of referenced items.
    final BinaryBloomFilter referencedAssets = BinaryBloomFilter.aggregate(assets.filter("referenced = true").select("data"), "data");

    // generate list of maybe referenced assets (note that a single asset may be referenced by both referenced and non-referenced values).
    // TODO: convert this to a group by asset, date and then figure out the latest date so we can include that
    // in the written file to avoid stale -> not stale -> stale values.
    Dataset<Row> unreferencedAssets = assets.filter("referenced = false").select("data").filter(new AssetFilter(referencedAssets));

    return unreferencedAssets.map(new UnreferencedItemConverter(valueWorker.getAssetKeySerializer()), Encoders.bean(UnreferencedItem.class));
  }


  public static class AssetFilter implements FilterFunction<Row> {
    private BinaryBloomFilter filter;

    public AssetFilter() {
    }

    public AssetFilter(BinaryBloomFilter filter) {
      this.filter = filter;
    }

    @Override
    public boolean call(Row r) throws Exception {
      byte[] bytes = r.getAs("data");
      return !filter.mightContain(bytes);
    }

  }

  public static class ConvertValues implements Function<InternalValue, ByteFrame>, Serializable {

    @Override
    public ByteFrame apply(InternalValue v) {
      return new ByteFrame(v.getBytes().toByteArray());
    }

  }

  public static final class CategorizedAssetKey implements Serializable {

    private boolean referenced;
    private byte[] data;

    public CategorizedAssetKey() {}

    public CategorizedAssetKey(boolean referenced, ByteString data) {
      super();
      this.referenced = referenced;
      this.data = data.toByteArray();
    }
    public void setReferenced(boolean referenced) {
      this.referenced = referenced;
    }
    public void setData(byte[] data) {
      this.data = data;
    }
    public boolean isReferenced() {
      return referenced;
    }
    public byte[] getData() {
      return data;
    }


  }

  public static class UnreferencedItemConverter implements Function1<Row, UnreferencedItem>, Serializable {

    private final Serializer<AssetKey> serializer;

    public UnreferencedItemConverter(Serializer<AssetKey> serializer) {
      this.serializer = serializer;
    }

    @Override
    public UnreferencedItem apply(Row r) {
      byte[] asset = r.getAs("data");
      AssetKey key = serializer.fromBytes(UnsafeByteOperations.unsafeWrap(asset));
      UnreferencedItem ui = new UnreferencedItem();
      ui.setName(key.toReportableName().stream().collect(Collectors.joining(".")));
      ui.setAsset(asset);
      return ui;
    }

  }

  public static class UnreferencedItem implements Serializable {
    private String name;
    private byte[] asset;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public byte[] getAsset() {
      return asset;
    }

    public void setAsset(byte[] asset) {
      this.asset = asset;
    }

  }

  public static class AssetCategorizer<T> implements FlatMapFunction<ByteFrame, CategorizedAssetKey> {

    private final BinaryBloomFilter bloomFilter;
    private final ValueWorker<T> valueWorker;

    public AssetCategorizer(BinaryBloomFilter bloomFilter, ValueWorker<T> valueWorker) {
      this.bloomFilter = bloomFilter;
      this.valueWorker = valueWorker;
    }

    @Override
    public Iterator<CategorizedAssetKey> call(ByteFrame r) throws Exception {
      InternalValue value = InternalValue.of(ByteString.copyFrom(r.getBytes()));
      T contents = valueWorker.fromBytes(value.getBytes());
      final Id valueId = value.getId();
      boolean referenced = bloomFilter.mightContain(valueId);
      final Serializer<AssetKey> serializer = valueWorker.getAssetKeySerializer();
      return valueWorker.getAssetKeys(contents).map(ak -> new CategorizedAssetKey(referenced, serializer.toBytes(ak))).iterator();
    }

  }

  public static class ByteFrame {
    private byte[] bytes;

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    public ByteFrame() {}

    public ByteFrame(byte[] bytes) {
      this.bytes = bytes;
    }

  }

}
