package org.apache.amoro.server.table;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.table.TableRuntimeFactory;
import org.apache.amoro.table.TableRuntimeStore;

public interface TableRuntimePlugin {

  boolean accept(ServerTableIdentifier tableIdentifier);

  void initialize(List<TableRuntime> tableRuntimes);

  void onTableCreated(@Nullable AmoroTable<?> amoroTable, TableRuntime tableRuntime);

  void onTableDropped(TableRuntime tableRuntime);

  void dispose();

  default TableRuntime createTableRuntime(TableRuntimeFactory.Creator creator, TableRuntimeStore store) {
    return creator.create(store);
  }
}
