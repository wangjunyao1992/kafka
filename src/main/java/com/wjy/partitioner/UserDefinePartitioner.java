package com.wjy.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月04日 18:44:00
 */
public class UserDefinePartitioner implements Partitioner {
    /**
     * 返回分区号   在这里写要分区的逻辑
     * @param s
     * @param o
     * @param bytes
     * @param o1
     * @param bytes1
     * @param cluster
     * @return
     */
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //这里根据需要把消息分配到不同的分区中去
        return 0;
    }

    /**
     * 关闭kafka生产者回调
     */
    @Override
    public void close() {
        System.out.println("close");
    }

    /**
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
