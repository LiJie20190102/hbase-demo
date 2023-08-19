package com.test.hbase.demo;

import com.test.hbase.exception.OperateHbaseException;
import com.test.hbase.pool.HbasePool;
import com.test.hbase.pool.PoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HBase统计数据表行数示例
 *
 * 命令行统计，耗时380秒：
 * $ hbase org.apache.hadoop.hbase.mapreduce.RowCounter 'user_info_test'
 *
 * @author wanggang
 *
 */
public class RowCounterDemo {

	public static void main(String[] args) throws IOException {
		/* 连接池配置 */
		PoolConfig config = new PoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000);
		config.setTestOnBorrow(true);

		/* Hbase配置 */
		Configuration hbaseConfig = HBaseConfiguration.create();

		/* 初始化连接池 */
		HbasePool pool = new HbasePool(config, hbaseConfig);

		/* 从连接池中获取对象 */
		Connection conn = pool.getConnection();

		// 表名，总共有一亿条数据
		TableName tableName = TableName.valueOf("qsdi_graph_worker0");

		/* 获取Admin对象 */
		try (Admin admin = conn.getAdmin();) {
			long start = System.currentTimeMillis();
			long rowCount = rowCountWithCoprocessor(admin, tableName, "describe");
			long end = System.currentTimeMillis();
			System.out.println("Admin统计行数：" + rowCount + "，统计时间：" + (end - start) + "毫秒.");
			// 输出结果：Admin统计行数：100000000，统计时间：51555毫秒.
		}

		// 获取某张数据表的行数
		try (Table table = conn.getTable(tableName);) {
			long start = System.currentTimeMillis();
			long rowCount = rowCountWithScanAndFilter(table);
			long end = System.currentTimeMillis();
			System.out.println("Table统计行数：" + rowCount + "，统计时间：" + (end - start) + "毫秒.");
			// 25分钟都没运行出来。。。
		}

		/* 返回连接资源 */
		pool.returnConnection(conn);

		/* 关闭连接池 */
		pool.close();
	}

	/**
	 * 使用协处理器新特性来对表行数进行统计
	 */
	public static long rowCountWithCoprocessor(Admin admin, TableName tableName, String family) {
		addTableCoprocessor(admin, tableName);
		try (AggregationClient ac = new AggregationClient(admin.getConfiguration());) {
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(family));
			long rowCount = 0;
			rowCount = ac.rowCount(tableName, new LongColumnInterpreter(), scan);
			return rowCount;
		} catch (Throwable e) {
			throw new OperateHbaseException(e.getMessage(),e);
		}
	}

	private static void addTableCoprocessor(Admin admin, TableName tableName) {
		try {
			String aggregateImplementation = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
			HTableDescriptor htd = admin.getTableDescriptor(tableName);
			List<String> coprocessors = htd.getCoprocessorDescriptors().stream().map(CoprocessorDescriptor::getClassName).collect(Collectors.toList());
			if (coprocessors.contains(aggregateImplementation)) {
				return;
			}
			admin.disableTable(tableName);
			HTableDescriptor hTableDescriptor = HTableDescriptor.parseFrom(htd.toByteArray());

			// 协处理器可以选择
			hTableDescriptor.addCoprocessor(aggregateImplementation);
			admin.modifyTable(tableName, hTableDescriptor);
			admin.enableTable(tableName);
		} catch (IOException e) {
			throw new OperateHbaseException(e.getMessage(),e);
		} catch (DeserializationException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 使用Scan与Filter的方式对表行数进行统计
	 */
	public static long rowCountWithScanAndFilter(Table table) {
		long rowCount = 0;
		try {
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());

			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				rowCount += result.size();
			}
		} catch (IOException e) {
			throw new RuntimeException();
		}
		return rowCount;
	}

}
