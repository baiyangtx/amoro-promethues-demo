package org.example;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TableOptimizingTotalCollector extends TableCollector<TableOptimizingTotalCollector.TableOptimizingCount> {

  public static final String NAME = "table_optimizing_total";
  public static final List<String> labels = ImmutableList.of("table_name", "optimizing_type", "is_success");

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples.Sample> samples = Lists.newArrayList();
    for (String tableName: tableMetric.keySet()) {
      TableOptimizingCount count = tableMetric.get(tableName);
      for (String taskType: Constants.tasks) {
        MetricFamilySamples.Sample sample = new MetricFamilySamples.Sample(
            NAME,
            labels,
            ImmutableList.of(tableName, taskType, "true"),
            count.taskCounts.get(taskType).success.get()
        );
        samples.add(sample);

        sample = new MetricFamilySamples.Sample(
            NAME,
            labels,
            ImmutableList.of(tableName, taskType, "false"),
            count.taskCounts.get(taskType).failed.get()
        );
        samples.add(sample);
      }

    }
    return Lists.newArrayList(
        new MetricFamilySamples(NAME, Type.COUNTER, "table optimizing total counter", samples)
    );
  }

  @Override
  protected TableOptimizingCount newTableMetric() {
    System.out.println("new table optimizing count added");
    return new TableOptimizingCount();
  }

  public void tableProcessFinished(String tableName, String processType, boolean success) {
    System.out.printf("table process finished: %s %s %s \n", tableName, processType, success);
    tableMetric.computeIfPresent(tableName, (k,m) -> {
      if (success) {
        m.taskCounts.get(processType).success.incrementAndGet();
      } else {
        m.taskCounts.get(processType).failed.incrementAndGet();
      }

      return m;
    });
  }

  public static final class TableOptimizingCount {
    Map<String, ProcessCount> taskCounts = Maps.newConcurrentMap();

    public TableOptimizingCount() {
      for (String type : Constants.tasks) {
        taskCounts.put(type, new ProcessCount());
      }
    }

    static final class ProcessCount {
      AtomicInteger success = new AtomicInteger(0);
      AtomicInteger failed = new AtomicInteger(0);
    }
  }
}
