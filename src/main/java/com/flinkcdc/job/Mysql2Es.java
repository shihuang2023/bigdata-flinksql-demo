package com.flinkcdc.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description TODO
 * @Author zwy
 * @Date 2023/7/21 9:02
 * @Version 1.0
 */
public class Mysql2Es {

    public void startJob(){
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 设置1个并行源任务
            env.setParallelism(1);
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            // 数据源表
            String sourceDDL = "CREATE TABLE IF NOT EXISTS user_view ( " +
                    "    id BIGINT, " +
                    "    name VARCHAR(30), " +
                    "    age INT, " +
                    "    email VARCHAR(50), " +
                    "    PRIMARY KEY(id) NOT ENFORCED " +
                    ") WITH ( " +
                    "    'connector' = 'mysql-cdc', " +
                    "    'hostname' = '172.18.1.65', " +
                    "    'port' = '3306', " +
                    "    'username' = 'flinkcdc', " +
                    "    'password' = '123456', " +
                    "    'database-name' = 'amtdatabase', " +
                    "    'table-name' = 'user' " +
                    ")";
            // 输出目标表
            String sinkDDL = "CREATE TABLE IF NOT EXISTS user_es_view ( " +
                    "    id BIGINT,                     " +
                    "    name VARCHAR(30)," +
                    "    age INT," +
                    "    email VARCHAR(50)," +
                    "    PRIMARY KEY(id) NOT ENFORCED " +
                    ") WITH ( " +
                    "    'connector' = 'elasticsearch-7', " +
                    "    'hosts' = 'http://172.18.0.53:9200', " +
                    "    'index' = 'user_es', " +
                    "    'sink.bulk-flush.max-actions' = '1' ) ";
            // 简单的聚合处理
            String transformSQL = "INSERT INTO user_es_view SELECT * FROM user_view";
            System.out.println("============================");
            tableEnv.executeSql(sourceDDL);
            tableEnv.executeSql(sinkDDL);
            TableResult result = tableEnv.executeSql(transformSQL);
            result.print();
            env.executeAsync("mysql-cdc-es");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }






//    public static void main(String[] args) throws Exception {
//        try {
//            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            // 设置1个并行源任务
//            env.setParallelism(1);
//            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//            // 数据源表
//            String sourceDDL = "CREATE TABLE IF NOT EXISTS user_view ( " +
//                    "    id BIGINT, " +
//                    "    name VARCHAR(30), " +
//                    "    age INT, " +
//                    "    email VARCHAR(50), " +
//                    "    PRIMARY KEY(id) NOT ENFORCED " +
//                    ") WITH ( " +
//                    "    'connector' = 'mysql-cdc', " +
//                    "    'hostname' = '172.18.1.65', " +
//                    "    'port' = '3306', " +
//                    "    'username' = 'flinkcdc', " +
//                    "    'password' = '123456', " +
//                    "    'database-name' = 'amtdatabase', " +
//                    "    'table-name' = 'user' " +
//                    ")";
//            // 输出目标表
//            String sinkDDL = "CREATE TABLE IF NOT EXISTS user_es_view ( " +
//                    "    id BIGINT,                     " +
//                    "    name VARCHAR(30)," +
//                    "    age INT," +
//                    "    email VARCHAR(50)," +
//                    "    PRIMARY KEY(id) NOT ENFORCED " +
//                    ") WITH ( " +
//                    "    'connector' = 'elasticsearch-7', " +
//                    "    'hosts' = 'http://172.18.0.53:9200', " +
//                    "    'index' = 'user_es', " +
//                    "    'sink.bulk-flush.max-actions' = '1' ) ";
//            // 简单的聚合处理
//            String transformSQL = "INSERT INTO user_es_view SELECT * FROM user_view";
//            System.out.println("============================");
//            tableEnv.executeSql(sourceDDL);
//            tableEnv.executeSql(sinkDDL);
//            TableResult result = tableEnv.executeSql(transformSQL);
//            result.print();
//            env.executeAsync("mysql-cdc-es");
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//    }
}
