package org.example;

import java.util.Random;

import static org.example.Constants.stateChange;
import static org.example.Constants.tasks;

public class TableOptimizingMockBackend implements Runnable {



  volatile boolean stop = false;
  String state = "idle";

  final String tableName;
  public TableOptimizingMockBackend(String tableName) {
    this.tableName = tableName;
    System.out.println(tableName + ": TableOptimizingMockBackend");
  }

  public void stop() {
    this.stop = true;
  }

  @Override
  public void run() {
    System.out.println(tableName + ": mock backend start");

    try {
      Random random = new Random(tableName.hashCode());
      MetricManager.getInstance().createTable(tableName);

      while (!stop) {
        int sleep = random.nextInt(10);
        try {
          Thread.sleep(sleep * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        state = stateChange.get(state);
        MetricManager.getInstance().tableStateChange(tableName, state);
        if ("idle".equalsIgnoreCase(state)) {
          // commit to idle,
          String task = tasks[random.nextInt(3)];
          boolean success = random.nextInt(10) > 7;
          MetricManager.getInstance().tableProcessFinished(tableName, task, success);
        }
      }
      MetricManager.getInstance().removeTable(tableName);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }
}
