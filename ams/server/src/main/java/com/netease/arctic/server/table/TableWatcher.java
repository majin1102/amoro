package com.netease.arctic.server.table;

import com.netease.arctic.AmoroTable;
import com.netease.arctic.ams.api.config.TableConfiguration;

public interface TableWatcher {

  // TODO wangtaohz: useless?
  void start();

  void tableAdded(DefaultTableRuntime tableRuntime, AmoroTable<?> table);

  void tableRemoved(DefaultTableRuntime tableRuntime);

  // TODO wangtaohz: useless?
  void tableChanged(DefaultTableRuntime tableRuntime, TableConfiguration oldConfig);
}
