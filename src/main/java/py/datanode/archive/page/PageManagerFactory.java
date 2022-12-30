/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.datanode.archive.page;

import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.Validate;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.segment.SegmentUnitManager;
import py.exception.StorageException;

public abstract class PageManagerFactory<P extends Page> {
  protected final DataNodeConfiguration options;

  public PageManagerFactory(DataNodeConfiguration options) throws IOException {
    Validate.isTrue(options != null);
    this.options = options;
  }

  public abstract PageManager<P> build(File mmppDir, SegmentUnitManager segmentUnitManager)
      throws StorageException;

}
