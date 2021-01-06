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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.store.Id;

public class L1Frame implements Serializable {

  private static final long serialVersionUID = 2021838527611235712L;

  private IdFrame id;
  private long dt;
  private List<IdFrame> children;
  private List<L1Parent> parents;

  public IdFrame getId() {
    return id;
  }

  public void setId(IdFrame id) {
    this.id = id;
  }

  public long getDt() {
    return dt;
  }

  public void setDt(long dt) {
    this.dt = dt;
  }

  public List<L1Parent> getParents() {
    return parents;
  }

  public void setParents(List<L1Parent> parents) {
    this.parents = parents;
  }

  public List<IdFrame> getChildren() {
    return children;
  }

  public void setChildren(List<IdFrame> children) {
    this.children = children;
  }

  public L1Frame() {
  }

  public L1Frame(Id id, long dt, List<Id> parents, List<Id> children) {
    this.id = IdFrame.of(id);
    this.dt = dt;
    List<L1Parent> values = new ArrayList<L1Parent>(parents.size());
    for (int i = 0; i < parents.size(); i++) {
      values.add(L1Parent.of(parents.get(i), i == parents.size() - 1));
    }
    this.parents = values;
    this.children = children.stream().map(IdFrame::of).collect(Collectors.toList());
  }

  public static class L1Parent implements Serializable {
    private static final long serialVersionUID = 2264533513613785665L;

    private IdFrame id;
    private boolean recurse;

    public static L1Parent of(Id id, boolean last) {
      L1Parent child = new L1Parent();
      child.id = IdFrame.of(id);
      child.recurse = last;
      return child;
    }

    public IdFrame getId() {
      return id;
    }

    public void setId(IdFrame id) {
      this.id = id;
    }

    public boolean isRecurse() {
      return recurse;
    }

    public void setRecurse(boolean recurse) {
      this.recurse = recurse;
    }

    @Override
    public String toString() {
      return "L1Child [id=" + id + ", recurse=" + recurse + "]";
    }


  }

}
