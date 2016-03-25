package com.qijiabin.test;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

public class OpenReplicatorTest {
	
	public static void main(String args[]) throws Exception {
		final OpenReplicator or = new OpenReplicator();
		or.setUser("root");
		or.setPassword("root");
		or.setHost("localhost");
		or.setPort(3306);
		or.setServerId(1);
		or.setBinlogPosition(5);
		or.setBinlogFileName("logbin.000001");
		or.setBinlogEventListener(new BinlogEventListener() {
		    public void onEvents(BinlogEventV4 event) {
				System.out.println(event);
		    }
		});
		or.start();
	}
	
}