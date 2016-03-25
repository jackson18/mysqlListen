package com.qijiabin.auto.openReplicator;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.code.or.OpenReplicator;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ResultSetRowPacket;
import com.google.code.or.net.impl.packet.command.ComQuery;

/**
 * ========================================================
 * 日 期：2016年3月25日 上午10:15:56
 * 作 者：jiabin.qi
 * 版 本：1.0.0
 * 类说明：MySQL binlog分析程序 ,用到open-replicator包
 * 增加加自动配置binlog位置及重连机制
 * ========================================================
 * 修订日期     修订人    描述
 */
public class AutoOpenReplicator extends OpenReplicator {

    private boolean autoReconnect = true;
    private int delayReconnect = 30;
    private int defaultTimeout = 10 * 1000;
    private long lastAlive;
    private Transport comQueryTransport;

    
    /**
     * 是否自动重连
     * @return 自动重连
     */
    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    /**
     * 设置自动重连
     * @param autoReconnect 自动重连
     */
    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    /**
     * 断开多少秒后进行自动重连
     * @param delayReconnect 断开后多少秒
     */
    public void setDelayReconnect(int delayReconnect) {
        this.delayReconnect = delayReconnect;
    }

    /**
     * 断开多少秒后进行自动重连
     * @return 断开后多少秒
     */
    public int getDelayReconnect() {
        return delayReconnect;
    }

    @Override
    public void start() {
        do {
            try {
                long current = System.currentTimeMillis();
                if (!this.isRunning()) {
                    if (this.getBinlogFileName() == null) {
                    	updatePosition();
                    }
                    System.out.println("准备分析的binlog文件为:" + this.binlogFileName+",分析位置为："+this.binlogPosition);
                    this.reset();
                    super.start();
                    System.out.println("启动成功!" + this.defaultTimeout/1000 + " 秒后若无事件发生将进行重连!");
                } else {
                    if (current - this.lastAlive >= this.defaultTimeout) {
                        this.stopQuietly(0, TimeUnit.SECONDS);
                        updatePosition();
                        this.lastAlive = System.currentTimeMillis();
                    }
                }
                TimeUnit.SECONDS.sleep(this.getDelayReconnect());
            } catch (Exception e) {
                try {
                	// reconnect failure, reget last binlog & position from master node and update cache!
                	//LoadCenter.loadAll(); // just update all cache, not flush!
                	updatePosition();
                    TimeUnit.SECONDS.sleep(this.getDelayReconnect());
                } catch (InterruptedException ignore) {
                    System.out.println(ignore);
                }
            }
        } while (this.autoReconnect);
    }

    @Override
    public void stopQuietly(long timeout, TimeUnit unit) {
        super.stopQuietly(timeout, unit);
        if (this.getBinlogParser() != null) {
            // 重置, 当MySQL服务器进行restart/stop操作时进入该流程
            this.binlogParser.setParserListeners(null); // 这句比较关键，不然会死循环
        }
    }

    /**
     * 自动配置binlog位置
     */
    private void updatePosition() {
        // 配置binlog位置
        try {
            ResultSetRowPacket binlogPacket = query("show master status");
            if (binlogPacket != null) {
                List<StringColumn> values = binlogPacket.getColumns();
                this.setBinlogFileName(values.get(0).toString());
                this.setBinlogPosition(Long.valueOf(values.get(1).toString()));
            }
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("update binlog position failure!");
        }
    }

    /**
     * ComQuery 查询
     * @param sql 查询语句
     * @return
     */
    private ResultSetRowPacket query(String sql) throws Exception {
        ResultSetRowPacket row = null;
        final ComQuery command = new ComQuery();
        command.setSql(StringColumn.valueOf(sql.getBytes()));
        if (this.comQueryTransport == null) this.comQueryTransport = getDefaultTransport();
        this.comQueryTransport.connect(this.host, this.port);
        this.comQueryTransport.getOutputStream().writePacket(command);
        this.comQueryTransport.getOutputStream().flush();
        // step 1
        this.comQueryTransport.getInputStream().readPacket();
        //
        Packet packet;
        // step 2
        while (true) {
            packet = comQueryTransport.getInputStream().readPacket();
            if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
                break;
            }
        }
        // step 3
        while (true) {
            packet = comQueryTransport.getInputStream().readPacket();
            if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
                break;
            } else {
                row = ResultSetRowPacket.valueOf(packet);
            }
        }
        this.comQueryTransport.disconnect();
        return row;
    }

    private void reset() {
        this.transport = null;
        this.binlogParser = null;
    }
    
}

