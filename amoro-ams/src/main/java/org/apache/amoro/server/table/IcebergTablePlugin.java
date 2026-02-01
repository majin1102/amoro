package org.apache.amoro.server.table;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.table.TableRuntimeFactory;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.arrow.util.Preconditions;

public class IcebergTablePlugin implements TableRuntimePlugin, TableRuntimeHandler {

  private final RuntimeHandlerChain headHandler;

  private IcebergTablePlugin(RuntimeHandlerChain headHandler) {
    Preconditions.checkNotNull(headHandler);
    this.headHandler = headHandler;
  }

  @Override
  public boolean accept(ServerTableIdentifier tableIdentifier) {
    return tableIdentifier.getFormat() == TableFormat.ICEBERG
        || tableIdentifier.getFormat() == TableFormat.MIXED_HIVE
        || tableIdentifier.getFormat() == TableFormat.MIXED_ICEBERG;
  }

  @Override
  public TableRuntime createTableRuntime(TableRuntimeFactory.Creator creator, TableRuntimeStore store) {
    if (!(store instanceof DefaultTableRuntimeStore)) {
      throw new IllegalStateException("Only support DefaultTableRuntimeStore");
    }
    DefaultTableRuntimeStore icebergRuntimeStore = (DefaultTableRuntimeStore) store;
    icebergRuntimeStore.setRuntimeHandler(this);
    return creator.create(store);
  }

  @Override
  public void initialize(List<TableRuntime> tableRuntimes) {
    if (headHandler != null) {
      headHandler.initialize(tableRuntimes);
    }
  }

  @Override
  public void onTableCreated(@Nullable AmoroTable<?> amoroTable, TableRuntime tableRuntime) {
    if (headHandler != null) {
      headHandler.fireTableAdded(amoroTable,tableRuntime);
    }
  }

  @Override
  public void onTableDropped(TableRuntime tableRuntime) {
    if (headHandler != null) {
      headHandler.fireTableRemoved(tableRuntime);
    }
  }

  @Override
  public void dispose() {
    if (headHandler != null) {
      headHandler.dispose();
    }
  }

  public void handleTableChanged(TableRuntime tableRuntime, OptimizingStatus originalStatus) {
    if (headHandler != null) {
      headHandler.fireStatusChanged(tableRuntime, originalStatus);
    }
  }

  public void handleTableChanged(TableRuntime tableRuntime, TableConfiguration originalConfig) {
    if (headHandler != null) {
      headHandler.fireConfigChanged(tableRuntime, originalConfig);
    }
  }

  public static IcebergTablePluginBuilder builder() {
    return new IcebergTablePluginBuilder();
  }

  public static class IcebergTablePluginBuilder {
    private RuntimeHandlerChain headHandler;

    private IcebergTablePluginBuilder() {
    }

    public IcebergTablePluginBuilder addHandler(RuntimeHandlerChain handler) {
      if (headHandler == null) {
        headHandler = handler;
      } else {
        headHandler.appendNext(handler);
      }
      return this;
    }

    public IcebergTablePlugin build() {
      return new IcebergTablePlugin(headHandler);
    }
  }
}
