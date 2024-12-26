

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
