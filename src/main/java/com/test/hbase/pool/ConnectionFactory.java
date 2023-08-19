package com.test.hbase.pool;

import org.apache.commons.pool2.PooledObjectFactory;

import java.io.Serializable;

/**
 * 连接工厂接口
 *
 * @author wanggang
 *
 * @param <T>
 */
public interface ConnectionFactory<T> extends PooledObjectFactory<T>, Serializable {

	/**
	 * 创建连接
	 *
	 * @return 连接
	 * @throws Exception
	 */
	public abstract T createConnection() throws Exception;

}
