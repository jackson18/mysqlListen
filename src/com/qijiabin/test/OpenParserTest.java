package com.qijiabin.test;

import java.io.IOException;

import com.google.code.or.OpenParser;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

public class OpenParserTest {
	
	public static void main(String[] args) {
		try {
			final OpenParser op = new OpenParser();
			op.setStartPosition(293);
			op.setBinlogFileName("logbin.000002");
			op.setBinlogFilePath("D:/soft/log-bin");
			op.setBinlogEventListener(new BinlogEventListener() {
			    public void onEvents(BinlogEventV4 event) {
			    	System.out.println(event);
			    }
			});
			op.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
}
