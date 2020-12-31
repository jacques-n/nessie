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
package com.dremio.nessie.tiered.builder;

import com.dremio.nessie.versioned.Key;

public interface FragmentConsumer<T extends FragmentConsumer<T>> extends HasIdConsumer<T> {

  /**
   * The commit metadata id for this l1.
   *
   * @param key The id to add.
   * @return This consumer.
   */
  T addKey(Key key);
}