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
package com.dremio.nessie.versioned.impl;

import java.util.Iterator;
import java.util.Map;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.google.common.collect.Iterators;

public class L2 extends MemoizedId implements Iterable<Id> {

  private static final long HASH_SEED = -6352836103271505167L;

  public static final int SIZE = 199;
  public static L2 EMPTY = new L2(null, new IdMap(SIZE, L3.EMPTY_ID), 0);
  public static Id EMPTY_ID = EMPTY.getId();

  private final IdMap map;
  private final long dt;

  private L2(Id id, IdMap map, long dt) {
    super(id);
    assert map.size() == SIZE;
    this.map = map;
    this.dt = dt;
  }

  private L2(IdMap map) {
    this(null, map, 0);
  }


  Id getId(int position) {
    return map.getId(position);
  }

  L2 set(int position, Id l2Id) {
    return new L2(map.withId(position, l2Id));
  }

  @Override
  Id generateId() {
    return Id.build(h -> {
      h.putLong(HASH_SEED);
      map.forEach(id -> h.putBytes(id.getValue().asReadOnlyByteBuffer()));
    });
  }

  public static final SimpleSchema<L2> SCHEMA = new SimpleSchema<L2>(L2.class) {

    private static final String ID = "id";
    private static final String TREE = "tree";

    @Override
    public L2 deserialize(Map<String, Entity> attributeMap) {
      return new L2(
          Id.fromEntity(attributeMap.get(ID)),
          IdMap.fromEntity(attributeMap.get(TREE), SIZE),
          DTMap.from(attributeMap)
      );
    }

    @Override
    public Map<String, Entity> itemToMap(L2 item, boolean ignoreNulls) {
      return DTMap.create()
          .put(TREE, item.map.toEntity())
          .put(ID, item.getId().toEntity())
          .build();
    }

  };


  @Override
  public Iterator<Id> iterator() {
    return Iterators.unmodifiableIterator(map.iterator());
  }

  /**
   * return the number of positions that are non-empty.
   * @return number of non-empty positions.
   */
  int size() {
    int count = 0;
    for (Id id : map) {
      if (!id.equals(L3.EMPTY_ID)) {
        count++;
      }
    }
    return count;
  }
}
