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

/**
 * Determines whether a particular L1 is referenced.
 */
public interface GcPolicy extends Serializable {

  boolean isReferencedL1(String name, long dt);

  static GcPolicy of(GcOptions options) {
    return new GcPolicy() {

      @Override
      public boolean isReferencedL1(String name, long dt) {
        return true;
      }
    };
  }

}
