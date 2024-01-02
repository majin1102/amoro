package com.netease.arctic.utils;

import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.StructLikeMap;

import java.util.List;
import java.util.function.Predicate;

public class ArcticTableUtil {
  public static final String BLOB_TYPE_OPTIMIZED_SEQUENCE = "optimized-sequence";
  public static final String BLOB_TYPE_BASE_OPTIMIZED_TIME = "base-optimized-time";

  public static final String BLOB_TYPE_OPTIMIZED_SEQUENCE_EXIST = "optimized-sequence.exist";
  public static final String BLOB_TYPE_BASE_OPTIMIZED_TIME_EXIST = "base-optimized-time.exist";

  /** Return the base store of the arctic table. */
  public static UnkeyedTable baseStore(ArcticTable arcticTable) {
    if (arcticTable.isKeyedTable()) {
      return arcticTable.asKeyedTable().baseTable();
    } else {
      return arcticTable.asUnkeyedTable();
    }
  }

  /** Return the table root location of the arctic table. */
  public static String tableRootLocation(ArcticTable arcticTable) {
    String tableRootLocation;
    if (TableFormat.ICEBERG != arcticTable.format() && arcticTable.isUnkeyedTable()) {
      tableRootLocation = TableFileUtil.getFileDir(arcticTable.location());
    } else {
      tableRootLocation = arcticTable.location();
    }
    return tableRootLocation;
  }

  public static StructLikeMap<Long> readOptimizedSequence(KeyedTable table) {
    return readWithLegacy(table.baseTable(), null, BLOB_TYPE_OPTIMIZED_SEQUENCE);
  }

  public static StructLikeMap<Long> readOptimizedSequence(KeyedTable table, long snapshotId) {
    return readWithLegacy(table.baseTable(), snapshotId, BLOB_TYPE_OPTIMIZED_SEQUENCE);
  }

  public static StructLikeMap<Long> readBaseOptimizedTime(KeyedTable table) {
    return readWithLegacy(table.baseTable(), null, BLOB_TYPE_BASE_OPTIMIZED_TIME);
  }

  public static StructLikeMap<Long> readBaseOptimizedTime(KeyedTable table, long snapshotId) {
    return readWithLegacy(table.baseTable(), snapshotId, BLOB_TYPE_BASE_OPTIMIZED_TIME);
  }

  private static StructLikeMap<Long> readWithLegacy(
      UnkeyedTable table, Long snapshotId, String type) {
    if (snapshotId == null) {
      Snapshot snapshot = table.currentSnapshot();
      if (snapshot == null) {
        return readLegacyPartitionProperties(table, type);
      } else {
        snapshotId = snapshot.snapshotId();
      }
    }
    StructLikeMap<Long> result = readFromStatisticsFile(table, snapshotId, type);
    return result != null ? result : readLegacyPartitionProperties(table, type);
  }

  private static StructLikeMap<Long> readFromStatisticsFile(
      UnkeyedTable table, long snapshotId, String type) {
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "Snapshot %s not found", snapshotId);
    Snapshot snapshotContainsType = findLatestValidSnapshot(table, snapshotId, isTypeExist(type));
    if (snapshotContainsType == null) {
      // Return null to read from legacy partition properties
      return null;
    }
    List<StatisticsFile> statisticsFiles =
        StatisticsFileUtil.getStatisticsFiles(table, snapshotContainsType.snapshotId(), type);
    Preconditions.checkState(
        !statisticsFiles.isEmpty(), "Statistics file not found for snapshot %s", snapshotId);
    Preconditions.checkState(
        statisticsFiles.size() == 1,
        "There should be only one statistics file for snapshot %s",
        snapshotId);
    List<StructLikeMap<Long>> result =
        StatisticsFileUtil.reader(table)
            .read(
                statisticsFiles.get(0),
                type,
                StatisticsFileUtil.createPartitionDataSerializer(table.spec(), Long.class));
    if (result.size() != 1) {
      throw new IllegalStateException(
          "There should be only one partition data in statistics file for blob type " + type);
    }
    return result.get(0);
  }

  /**
   * Find the latest valid snapshot which satisfies the condition.
   *
   * @param table - Iceberg table
   * @param currentSnapshotId - find from this snapshot
   * @param condition - the condition to satisfy
   * @return the latest valid snapshot or null if not exists
   */
  public static Snapshot findLatestValidSnapshot(
      Table table, long currentSnapshotId, Predicate<Snapshot> condition) {
    Long snapshotId = currentSnapshotId;
    while (true) {
      if (snapshotId == null || table.snapshot(snapshotId) == null) {
        return null;
      }
      Snapshot snapshot = table.snapshot(snapshotId);
      if (condition.test(snapshot)) {
        return snapshot;
      } else {
        // seek parent snapshot
        snapshotId = snapshot.parentId();
      }
    }
  }

  @VisibleForTesting
  public static Predicate<Snapshot> isTypeExist(String type) {
    switch (type) {
      case BLOB_TYPE_OPTIMIZED_SEQUENCE:
        return snapshot -> snapshot.summary().containsKey(BLOB_TYPE_OPTIMIZED_SEQUENCE_EXIST);
      case BLOB_TYPE_BASE_OPTIMIZED_TIME:
        return snapshot -> snapshot.summary().containsKey(BLOB_TYPE_BASE_OPTIMIZED_TIME_EXIST);
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  private static StructLikeMap<Long> readLegacyPartitionProperties(
      UnkeyedTable table, String type) {
    // To be compatible with old Amoro version 0.5.0 which didn't use puffin file and stored the
    // statistics in table properties
    switch (type) {
      case BLOB_TYPE_OPTIMIZED_SEQUENCE:
        return TablePropertyUtil.getPartitionLongProperties(
            table, TableProperties.PARTITION_OPTIMIZED_SEQUENCE);
      case BLOB_TYPE_BASE_OPTIMIZED_TIME:
        return TablePropertyUtil.getPartitionLongProperties(
            table, TableProperties.PARTITION_BASE_OPTIMIZED_TIME);
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  /**
   * Return the {@link PartitionSpec} of the arctic table by {@link PartitionSpec#specId()}, Mix
   * format table will return directly after checking}.
   */
  public static PartitionSpec getArcticTablePartitionSpecById(ArcticTable arcticTable, int specId) {
    if (arcticTable.format() == TableFormat.ICEBERG) {
      return arcticTable.asUnkeyedTable().specs().get(specId);
    } else {
      PartitionSpec spec = arcticTable.spec();
      if (spec.specId() != specId) {
        throw new IllegalArgumentException(
            "Partition spec id " + specId + " not found in table " + arcticTable.name());
      }
      return spec;
    }
  }
}
