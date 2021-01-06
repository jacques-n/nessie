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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.LocalDynamoDB;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.StringSerializer;
import com.dremio.nessie.versioned.ValueWorker;
import com.dremio.nessie.versioned.impl.TieredVersionStore;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;
import com.dremio.nessie.versioned.store.dynamo.DynamoStoreConfig;
import com.dremio.nessie.versioned.tests.CommitBuilder;
import com.dremio.versioned.IdentifyUnreferencedAssets.UnreferencedItem;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Funnel;
import com.google.protobuf.ByteString;

@ExtendWith(LocalDynamoDB.class)
public class TestGC {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private StoreWorker<Value,String> helper;
  private DynamoStore store;
  private TieredVersionStore<Value, String> versionStore;

  @Test
  public void run() throws Exception {

    System.out.println(versionStore.getNamedRefs().collect(Collectors.toList()));
    // commit one asset on main branch.
    BranchName main = BranchName.of("main");
    for(int i = 0; i < 25; i++) {
      commit().put("k1", new Value().add(i)).withMetadata("c2").toBranch(main);
    }

    // create a new branch, commit two assets, then delete the branch.
    BranchName toBeDeleted = BranchName.of("toBeDeleted");
    versionStore.create(toBeDeleted, Optional.empty());
    Hash h = commit().put("k1", new Value().add(1).add(2)).withMetadata("c1").toBranch(toBeDeleted);
    versionStore.delete(toBeDeleted, Optional.of(h));

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaSparkPi")
        .master("local")
        .getOrCreate();

    spark.sparkContext().setCheckpointDir("/tmp/spark");

    //RefToL2Producer.getL2BloomFilter(spark, new DynamoSupplier(), GcPolicy.of(null), 10_000_000);

    // now confirm that the unreferenced asset is marked for deletion.
    GcOptions options = ImmutableGcOptions.builder().bloomFilterCapacity(10_000_000).storeType("DYNAMO").build();
    IdentifyUnreferencedAssets<Value> app = new IdentifyUnreferencedAssets<Value>(helper, new DynamoSupplier(), spark, options);
    Dataset<UnreferencedItem> items = app.identify();
    items.show();
  }

  private static class DynamoSupplier implements Supplier<Store>, Serializable {

    private static final long serialVersionUID = 5030232198230089450L;

    @Override
    public Store get() {
      Store store;
      try {
        store = new DynamoStore(DynamoStoreConfig.builder().endpoint(new URI("http://localhost:8000")).build());
        store.start();
        return store;
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @BeforeEach
  void before() throws Exception {
    helper = new StoreW();
    store = new DynamoStore(DynamoStoreConfig.builder().endpoint(new URI("http://localhost:8000")).build());
    System.out.println("Store started");
    store.start();
    versionStore = new TieredVersionStore<>(helper, store, true);
    versionStore.create(BranchName.of("main"), Optional.empty());
  }

  @AfterEach
  void after() {
    store.deleteTables();
    store.close();
    helper = null;
    versionStore = null;
  }


  private CommitBuilder<Value, String> commit() {
    return new CommitBuilder<Value, String>(versionStore);
  }

  private static class StoreW implements StoreWorker<Value, String> {
    @Override
    public ValueWorker<Value> getValueWorker() {
      return new ValueValueWorker();
    }

    @Override
    public Serializer<String> getMetadataSerializer() {
      return StringSerializer.getInstance();
    }
  }

  private static class JsonSerializer<T> implements Serializer<T>, Serializable {

    private final Class<T> clazz;

    public JsonSerializer(Class<T> clazz) {
      this.clazz = clazz;
    }

    public Class<T> getSerializationClass() {
      return clazz;
    }

    @Override
    public ByteString toBytes(T value) {
      try {
        return ByteString.copyFrom(MAPPER.writer().writeValueAsBytes(value));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public T fromBytes(ByteString bytes) {
      try {
        return MAPPER.reader().readValue(bytes.toByteArray(), clazz);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static class ValueValueWorker extends JsonSerializer<Value> implements ValueWorker<Value>, Serializable {

    private static final long serialVersionUID = 3651529251225721177L;

    public ValueValueWorker() {
      super(Value.class);
    }

    @Override
    public Stream<? extends AssetKey> getAssetKeys(Value value) {
      return value.children.stream();
    }

    @Override
    public Funnel<Value> getFunnel() {
      return (value, sink) -> sink.putBytes(value.idAsBytes());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Serializer<AssetKey> getAssetKeySerializer() {
      return (Serializer<AssetKey>) (Object) new JsonSerializer<Child>(Child.class);
    }
  }

  private static class Value implements HasId {

    private final Id id;
    private final List<Child> children;

    @JsonCreator
    public Value(@JsonProperty("id") byte[] id, @JsonProperty("children") List<Child> children) {
      super();
      this.id = Id.of(id);
      this.children = children;
    }

    public Value() {
      this.id = Id.generateRandom();
      this.children = new ArrayList<>();
    }

    @JsonIgnore
    @Override
    public Id getId() {
      return id;
    }

    @JsonProperty("id")
    public byte[] idAsBytes() {
      return id.toBytes();
    }

    public Value add(int id) {
      children.add(new Child(id));
      return this;
    }

    public List<Child> getChildren() {
      return children;
    }
  }

  private static class Child extends AssetKey {

    private int id;

    @JsonCreator
    public Child(@JsonProperty("id") int id) {
      this.id = id;
    }

    public Child() {}

    public int getId() {
      return id;
    }

    @Override
    public CompletableFuture<Boolean> delete() {
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public List<String> toReportableName() {
      return Arrays.asList(Integer.toString(id));
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other instanceof Child && ((Child)other).id == id;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      id = in.readInt();
    }

  }


}
