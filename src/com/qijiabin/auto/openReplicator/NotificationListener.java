package com.qijiabin.auto.openReplicator;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.IntvarEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

/**
 * ========================================================
 * 日 期：2016年3月25日 上午10:00:02
 * 作 者：jiabin.qi
 * 版 本：1.0.0
 * 类说明：Binlog事件监听器模板
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class NotificationListener implements BinlogEventListener {
	
    private String eventDatabase;

    /**
     * 这里只是实现例子，该方法可以自由处理逻辑
     * @param event
     */
    @Override
    public void onEvents(BinlogEventV4 event) {
        Class<?> eventType = event.getClass();
        // 事务开始
	    if (eventType == QueryEvent.class) {
	        QueryEvent actualEvent = (QueryEvent) event;
	        this.eventDatabase = actualEvent.getDatabaseName().toString();
	        System.out.println("事件数据库名：" + eventDatabase);
	    }
        
        if (eventType == IntvarEvent.class) {
        	//新增（AUTO_INCREMENT ）
        	System.out.println("***********进行了自增操作*************");
        } else if (eventType == FormatDescriptionEvent.class) {
        	//查询
        	System.out.println("***********这表明一个日志文件以MySQL5或以后开始写的。*************");
        } else if (eventType == XidEvent.class) {
        	//结束事务（新增，更新，删除）
        	System.out.println("***********进行了增删改操作*************");
        }
    }
    
}

