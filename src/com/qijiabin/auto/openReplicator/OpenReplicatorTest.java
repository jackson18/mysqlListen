package com.qijiabin.auto.openReplicator;

/**
 * ========================================================
 * 日 期：2016年3月25日 上午10:15:10
 * 作 者：jiabin.qi
 * 版 本：1.0.0
 * 类说明：MySQL binlog分析程序测试
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class OpenReplicatorTest {
	
    public static void main(String args[]){
        // 配置从MySQL Master进行复制
        AutoOpenReplicator aor = new AutoOpenReplicator();
        aor.setServerId(1);
        aor.setHost("localhost");
        aor.setUser("root");
        aor.setPassword("root");
        aor.setAutoReconnect(true);
        aor.setDelayReconnect(5);
        aor.setBinlogEventListener(new NotificationListener());
        aor.start();
    }
    
}

