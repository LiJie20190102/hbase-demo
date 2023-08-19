/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.test.hbase.job;

import com.test.hbase.exception.OperateHbaseException;
import com.test.hbase.pool.HbasePool;
import com.test.hbase.pool.PoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Description
 *
 * @author lijie0203 2023/8/14 20:12
 */
public class HbaseCronJob implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseCronJob.class);
    private static final int NUM_THREAD = 5;

    private static HbasePool hbasePool = null;

    private static final String TABLE_PREFIX = "qsdi_graph_worker";
    private static final String ROW_KEY_PREFIX = "qsdi_graph_worker";

    public static void main(String[] args) throws IOException {
        ScheduledExecutorService executors = Executors.newScheduledThreadPool(NUM_THREAD);

        /* 连接池配置 */
        PoolConfig config = new PoolConfig();
        config.setMaxTotal(15);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        Configuration hbaseConfig = HBaseConfiguration.create();
        /* 初始化连接池 */
        hbasePool = new HbasePool(config, hbaseConfig);


        for (int i = 0; i < NUM_THREAD; i++) {
            /* 从连接池中获取对象 */
            Connection checkTableConn = hbasePool.getConnection();
            checkTable(checkTableConn, i);
            hbasePool.returnConnection(checkTableConn);
            int finalI = i;
            executors.scheduleAtFixedRate(() -> {
                insertData(finalI);
                scanStartData(finalI);
            }, 0, 30 + i * 2, TimeUnit.SECONDS);
        }

        /* 返回连接资源 */
//        hbasePool.returnConnection(conn);
        /* 关闭连接池 */
//        hbasePool.close();
    }

    private static void insertData(int finalI) {
        if (!insertTime()) {
            return;
        }
        Connection conn = hbasePool.getConnection();
        //需要获取一个table对象
        try (Table worker = conn.getTable(TableName.valueOf(TABLE_PREFIX + finalI))) {
            List<Put> puts = new ArrayList<>();
            //准备put对象
            for (int i = 0; i < 10; i++) {
                long timeMillis = System.currentTimeMillis();
                Put put = new Put(Bytes.toBytes(ROW_KEY_PREFIX + timeMillis));//指定rowkey
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"), Bytes.toBytes("qsdi hugegraph " + timeMillis));
                put.addColumn(Bytes.toBytes("describe"), Bytes.toBytes("desc"), Bytes.toBytes("qishu hugegraph " + timeMillis));
                puts.add(put);
            }

            //插入数据，参数类型是put
            worker.put(puts);
            //准备list<put> 可以执行批量插入

//            System.out.println(finalI + "插入数据到worker表成功");
        } catch (IOException e) {
            throw new OperateHbaseException(e.getMessage(), e);
        } finally {
            hbasePool.returnConnection(conn);
        }
    }


    public static void scanStartData(int finalI) {

        if (!queryTime()) {
            return;
        }
        Connection conn = hbasePool.getConnection();
        //准备table对象
        try (Table worker = conn.getTable(TableName.valueOf(TABLE_PREFIX + finalI))) {
            //准备Scan对象
            Scan scan = new Scan();
            //指定查询的rowkey区间,rowkey在hbase中是以字典序排列
            // 获取昨天凌晨的时间戳
            Instant yesterdayTimestamp = LocalDate.now().minusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant();
            scan.setStartRow(Bytes.toBytes(ROW_KEY_PREFIX + yesterdayTimestamp.toEpochMilli()));
            scan.setStopRow(Bytes.toBytes(ROW_KEY_PREFIX + System.currentTimeMillis()));
            //执行扫描
            ResultScanner scanner = worker.getScanner(scan);
            boolean first = true;
            for (Result result : scanner) {
                //获取到result中所有cell对象
                Cell[] cells = result.rawCells();
                //遍历打印
                Cell lastCell = cells[cells.length - 1];
                String rowkey = Bytes.toString(CellUtil.cloneRow(lastCell));
                String f = Bytes.toString(CellUtil.cloneFamily(lastCell));
                String column = Bytes.toString(CellUtil.cloneQualifier(lastCell));
                String value = Bytes.toString(CellUtil.cloneValue(lastCell));
                if (first) {
                    LOGGER.warn("rowkey-->" + rowkey + "--;cf-->" + f + "---;column" + column + "--;value" + value);
                    first = false;
                }

//                for (Cell cell : cells) {
//                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
//                    String f = Bytes.toString(CellUtil.cloneFamily(cell));
//                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    String value = Bytes.toString(CellUtil.cloneValue(cell));
//
//                    System.out.println("rowkey-->" + rowkey + "--;cf-->" + f + "---;column" + column + "--;value" + value);
//                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            hbasePool.returnConnection(conn);
        }

    }

    private static void checkTable(Connection conn, int i) {
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {
            String tableName = TABLE_PREFIX + i;
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            if (exists) {
                boolean tableAvailable = admin.isTableAvailable(TableName.valueOf(tableName));
                if (!tableAvailable) admin.enableTable(TableName.valueOf(tableName));
            } else {
                System.out.println(String.format("table %s does not exists, create it...", tableName));
                //创建Htabledesc描述器，表描述器
                HTableDescriptor worker = new HTableDescriptor(TableName.valueOf(tableName));
                //指定列族
                worker.addFamily(new HColumnDescriptor("info"));
                worker.addFamily(new HColumnDescriptor("describe"));
                admin.createTable(worker);
            }
        } catch (IOException e) {
            throw new OperateHbaseException(e.getMessage(), e);
        }
    }


    public static boolean insertTime() {
        LocalTime currentTime = LocalTime.now();

        LocalDate currentDate = LocalDate.now();
        DayOfWeek currentDay = currentDate.getDayOfWeek();
        // 判断是否为工作日
        if (currentDay != DayOfWeek.SATURDAY && currentDay != DayOfWeek.SUNDAY) {
            // 判断是否在9点至18点之间
            if (currentTime.isAfter(LocalTime.of(21, 0)) || currentTime.isBefore(LocalTime.of(5, 0))) {
                return true;
            }
        }
        return false;
    }

    public static boolean queryTime() {
        LocalTime currentTime = LocalTime.now();

        LocalDate currentDate = LocalDate.now();
        DayOfWeek currentDay = currentDate.getDayOfWeek();
        // 判断是否为工作日
        if (currentDay != DayOfWeek.SATURDAY && currentDay != DayOfWeek.SUNDAY) {
            // 判断是否在9点至18点之间
            if (currentTime.isAfter(LocalTime.of(9, 0)) && currentTime.isBefore(LocalTime.of(23, 0))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        if (null != hbasePool && !hbasePool.isClosed()) {
            hbasePool.close();
        }
    }
}
