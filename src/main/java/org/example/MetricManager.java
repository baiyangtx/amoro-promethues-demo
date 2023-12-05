package org.example;

public class MetricManager {

  private static final MetricManager instance = new MetricManager();

  public static MetricManager getInstance() {
    return instance;
  }



  TableOptimizingTotalCollector tableOptimizingTotal = new TableOptimizingTotalCollector()
      .register();

  TableOptimizingStatusDurationSeconds tableOptimizingStatusDurationSeconds = new TableOptimizingStatusDurationSeconds()
      .register();


  public void createTable(String tableName) {
    System.out.println("new table created: "+ tableName);
    tableOptimizingTotal.newTableAdded(tableName);
    tableOptimizingStatusDurationSeconds.newTableAdded(tableName);
  }

  public void removeTable(String tableName) {
    System.out.println("table remove: " + tableName);
    tableOptimizingTotal.tableRemoved(tableName);
    tableOptimizingStatusDurationSeconds.tableRemoved(tableName);
  }

  public void tableStateChange(String tableName, String newState){
    tableOptimizingStatusDurationSeconds.stateChange(tableName, newState);
  }


  public void tableProcessFinished(String tableName, String processType, boolean success) {
    tableOptimizingTotal.tableProcessFinished(tableName, processType, success);
  }

}
