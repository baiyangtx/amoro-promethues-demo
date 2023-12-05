package org.example;

import io.prometheus.client.Collector;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.util.List;

public class TableOptimizingStatusDurationSeconds extends TableCollector<TableOptimizingStatusDurationSeconds.Duration>{
  public static final String NAME = "table_optimizing_status_duration";
  public static final List<String> labels = ImmutableList.of("table_name", "optimizing_status");

  @Override
  protected Duration newTableMetric() {
    return new Duration();
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples.Sample> samples = Lists.newArrayList();
    tableMetric.forEach((tableName, duration) -> {
      samples.addAll(
          tableSamples(tableName, duration)
      );
    });
    return Lists.newArrayList(
        new MetricFamilySamples(
            NAME, Type.GAUGE, "table optimizing status duration", samples
        )
    );
  }

  private List<MetricFamilySamples.Sample> tableSamples(String tableName, Duration duration) {
    List<MetricFamilySamples.Sample> samples = Lists.newArrayList();
    String state = duration.currentState;
    int seconds = (int) ((System.currentTimeMillis() - duration.begin)/1000);
    for (String s: Constants.states) {
      int value = 0;
      if (s.equalsIgnoreCase(state)) {
        value = seconds;
      }
      samples.add(
          new MetricFamilySamples.Sample(
              NAME, labels,
              ImmutableList.of(tableName, s),
              value
          )
      );
    }
    return samples;
  }

  public void stateChange(String tableName, String state) {
    tableMetric.computeIfPresent(tableName, (n, duration) -> {
      duration.newState(state);
      return duration;
    });

  }

  public static class Duration {
    String currentState = "idle";
    long begin = System.currentTimeMillis();

    public void newState(String state) {
      this.currentState = state;
      this.begin = System.currentTimeMillis();
    }
  }
}
