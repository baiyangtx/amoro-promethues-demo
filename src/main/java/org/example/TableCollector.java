package org.example;

import io.prometheus.client.Collector;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;

public abstract class TableCollector<T> extends Collector {

  protected final Map<String, T> tableMetric = Maps.newConcurrentMap();

  public void newTableAdded(String tableName) {
    tableMetric.computeIfAbsent(tableName, k -> newTableMetric());
  }

  public void tableRemoved(String tableName) {
    tableMetric.remove(tableName);
  }

  protected abstract T newTableMetric() ;
}
