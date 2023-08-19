package com.test.hbase.exception;

/**
 * 连接异常
 *
 * @author wanggang
 *
 */
public class OperateHbaseException extends RuntimeException {

	private static final long serialVersionUID = -6503525110247209484L;

	public OperateHbaseException(String message) {
		super(message);
	}

	public OperateHbaseException(Throwable e) {
		super(e);
	}

	public OperateHbaseException(String message, Throwable cause) {
		super(message, cause);
	}

}
