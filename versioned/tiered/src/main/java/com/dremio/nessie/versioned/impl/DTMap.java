package com.dremio.nessie.versioned.impl;

import java.util.Map;
import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

/**
 * Responsible for storing microsecond timestamps of objects.
 */
public class DTMap {

  public static final String DT = "dt";

  private DTMap() {
  }

  public static long from(Map<String, Entity> map) {
    return map.getOrDefault(DT, Entity.ofNumber(0L)).getNumber();
  }

  public static Entity getNow() {
    // since we compile for java 8, Instance.now is not available, multiply by 1000 so we store microseconds for future compatibility.
    return Entity.ofNumber(System.currentTimeMillis() * 1000);
  }

  public static ImmutableMap.Builder<String, Entity> create() {
    return ImmutableMap.<String, Entity>builder().put(DT, Entity.ofNumber(0L) /** getNow() **/);
  }

}
