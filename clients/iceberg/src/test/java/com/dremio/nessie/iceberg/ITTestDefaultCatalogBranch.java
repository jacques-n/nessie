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

package com.dremio.nessie.iceberg;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.iceberg.NessieCatalog;

/**
 * test tag operations with a default tag set by server.
 */
class ITTestDefaultCatalogBranch extends BaseTestIceberg {

  public ITTestDefaultCatalogBranch() {
    super("main");
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void testBasicBranch() throws NessieNotFoundException, NessieConflictException {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    createTable(foobar, 1); //table 1
    createTable(foobaz, 1); //table 2

    catalog.refresh();
    tree.createNewBranch("FORWARD", catalog.getHash());
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_REF, "FORWARD");
    NessieCatalog forwardCatalog = new NessieCatalog(hadoopConfig);
    forwardCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    forwardCatalog.loadTable(foobar).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    Assertions.assertNotEquals(getContent(forwardCatalog, foobar),
                               getContent(catalog, foobar));
    Assertions.assertNotEquals(getContent(forwardCatalog, foobaz),
                               getContent(catalog, foobaz));

    System.out.println(getContent(forwardCatalog, foobar));
    System.out.println(getContent(catalog, foobar));

    forwardCatalog.refresh();
    tree.assignBranch("main", tree.getReferenceByName("main").getHash(), forwardCatalog.getHash());

    catalog.refresh();

    System.out.println(getContent(forwardCatalog, foobar));
    System.out.println(getContent(catalog, foobar));

    Assertions.assertEquals(getContent(forwardCatalog, foobar),
                            getContent(catalog, foobar));
    Assertions.assertEquals(getContent(forwardCatalog, foobaz),
                            getContent(catalog, foobaz));

    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    tree.deleteBranch("FORWARD", tree.getReferenceByName("FORWARD").getHash());
  }

}
