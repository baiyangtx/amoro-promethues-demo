package org.example;

import io.javalin.Javalin;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 *
 */
public class App 
{

    static Map<String, TableOptimizingMockBackend> tables = Maps.newConcurrentMap();
    static ExecutorService mockExecutor = Executors.newCachedThreadPool();

    public static void main( String[] args ) throws IOException {
        Javalin app = Javalin.create(config -> {

        });
        System.out.println(MetricManager.getInstance().getClass().getName());
        addTable("test1");
        addTable("test2");
        addTable("test3");
        addTable("test4");
        addTable("test5");


        app.get("/table", ctx -> {
            String tableName = ctx.req.getParameter("id");
            addTable(tableName);
            ctx.result("tableName: " + tableName);
        });

        app.get("/clear", ctx -> {
            String id = ctx.req.getParameter("id");
            removeTable(id);
        });

        HTTPServer prometheusServer = new HTTPServer(8002);
        System.out.println("start");
        app.start(8001);
    }

    public static void addTable(String tableName) {
        tables.computeIfAbsent(tableName, k -> {
            TableOptimizingMockBackend mockThread = new TableOptimizingMockBackend(tableName);
            mockExecutor.submit(mockThread);
            return mockThread;
        });
    }

    public static void removeTable(String tableName) {
        tables.computeIfPresent(tableName, (id, task)  -> {
            task.stop();
            return null;
        });
    }
}
