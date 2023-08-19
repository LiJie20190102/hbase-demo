/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.test.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Description
 *
 * @author lijie0203 2023/8/13 16:46
 */
public class HbaseClientTest {
    Configuration conf = null;
    Connection conn = null;

    @Before
    public void inin() throws IOException {
        //获取一个配置文件对象
        conf = HBaseConfiguration.create();
        //通过conf获取到hbase集群的连接
//        conf.set("hbase.zookeeper.quorum","linux121,linux122");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
        //通过conf获取到hbase集群的连接
        conn = ConnectionFactory.createConnection(conf);
    }

    //创建一张hbase表
    @Test
    public void createTable() throws IOException {
        //获取HbaseAdmin对象用来创建对象
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        //创建Htabledesc描述器，表描述器
        HTableDescriptor worker = new HTableDescriptor(TableName.valueOf("worker"));
        //指定列族
        worker.addFamily(new HColumnDescriptor("info"));
        admin.createTable(worker);
        System.out.println("worker表创建成功");
    }

    @Test
    public void listTables() throws IOException {
        //获取HbaseAdmin对象用来创建对象
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        System.out.println();

    }

    //插入一条数据
    @Test
    public void putData() throws IOException {
        //需要获取一个table对象
        Table worker = conn.getTable(TableName.valueOf("worker"));

        //准备put对象
        Put put = new Put(Bytes.toBytes("110"));//指定rowkey
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("addr"),Bytes.toBytes("beijing"));

        //插入数据，参数类型是put
        worker.put(put);
        //准备list<put> 可以执行批量插入

        //关闭table对象
        worker.close();
        System.out.println("插入数据到worker表成功");
    }

    //删除一条数据
    @Test
    public void delete() throws IOException {
        //需要获取一个table对象
        Table worker = conn.getTable(TableName.valueOf("worker"));

        //准备delete对象
        Delete delete = new Delete(Bytes.toBytes("110"));

        //执行删除
        worker.delete(delete);

        //关闭table对象
        worker.close();
        System.out.println("删除数据成功");
    }

    //查询数据
    @Test
    public void getData() throws IOException {
        //准备table对象
        Table worker = conn.getTable(TableName.valueOf("worker"));
        //准备get对象
        Get get = new Get(Bytes.toBytes("110"));
        //指定查询某个列族或列
        get.addFamily(Bytes.toBytes("info"));
        //执行查询
        Result result = worker.get(get);
        //获取到result中所有cell对象
        Cell[] cells = result.rawCells();
        //遍历打印
        for (Cell cell : cells) {
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            String f = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("rowkey-->"+rowkey+"--;cf-->"+f+"---;column-->"+column+"--;value:"+value);
        }

        worker.close();
    }

    //全表扫描
    @Test
    public void ScanData() throws IOException {
        //准备table对象
        Table worker = conn.getTable(TableName.valueOf("qsdi_graph_worker0"));
        //准备Scan对象
        Scan scan = new Scan();
        //执行扫描
        ResultScanner scanner = worker.getScanner(scan);
        for (Result result : scanner) {
            //获取到result中所有cell对象
            Cell[] cells = result.rawCells();
            //遍历打印
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("rowkey-->"+rowkey+"--;cf-->"+f+"---;column"+column+"--;value"+value);

            }
        }


        worker.close();
    }

    //指定scan开始rowkey和结束rowkey，这种查询方式建议使用，指定开始和结束rowkey区间避免全表扫描
    @Test
    public void ScanStartData() throws IOException {
        //准备table对象
        Table worker = conn.getTable(TableName.valueOf("qsdi_graph_worker0"));
        //准备Scan对象
        Scan scan = new Scan();
        //指定查询的rowkey区间,rowkey在hbase中是以字典序排列
        // 获取昨天凌晨的时间戳
        Instant yesterdayTimestamp = LocalDate.now().minusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant();
        scan.setStartRow(Bytes.toBytes("row_key_"+yesterdayTimestamp.toEpochMilli()));
        scan.setStopRow(Bytes.toBytes("row_key_"+System.currentTimeMillis()));
        //执行扫描
        ResultScanner scanner = worker.getScanner(scan);
        for (Result result : scanner) {
            //获取到result中所有cell对象
            Cell[] cells = result.rawCells();
            //遍历打印
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("rowkey-->"+rowkey+"--;cf-->"+f+"---;column"+column+"--;value"+value);
            }
        }


        worker.close();
    }


    //释放连接
    @After
    public void realse(){
        if (conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
