package com.wjy.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月05日 19:19:00
 */
public class UserDefineProducerInterceptor implements ProducerInterceptor {

    /**
     * 发送数据前调用
     * @param record
     * @return
     */
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return new ProducerRecord(record.topic(), record.key(), record.value() + "wangjunyao");
    }

    /**
     * 发送一条数据后，回调
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("metadata : " + metadata + ", exception : " + exception);
    }

    /**
     * 生产者关闭回调，可以释放资源
     */
    @Override
    public void close() {

    }

    /**
     * 生产者配置信息
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
