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
package org.projectnessie.versioned.tiered;

import java.util.stream.Stream;

import org.projectnessie.versioned.store.Id;

/**
 * Consumer for L2s.
 * <p>
 * Implementations must return a shared state ({@code this}) from its method.
 * </p>
 */
public interface L2 extends BaseValue<L2> {

  /**
   * Add a list of children ids indexed by position.
   * <p>Must be called exactly once.</p>
   *
   * @param ids The list of ids
   * @return This consumer.
   */
  default L2 children(Stream<Id> ids) {
    ids.forEach(ignored -> {});
    return this;
  }
}