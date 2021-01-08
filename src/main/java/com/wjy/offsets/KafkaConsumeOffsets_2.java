package com.wjy.offsets;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月03日 15:44:00
 */
public class KafkaConsumeOffsets_2 {

    public static void main(String[] args) {
        //创建kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "wangjunyao100.com:9092,wangjunyao101.com:9092,wangjunyao102.com:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "G2");

        //earliest  （第一次访问Kafka服务，没有该消费者组的 偏移量时起作用，一旦有了偏移量，下次就从偏移量开始读取）
        //自动将偏移量重置为最早的偏移量（从topic最早的记录开始读取）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //订阅相关的Topics
        //consumer.subscribe(Pattern.compile("^topic.*"));
        consumer.subscribe(Arrays.asList("topic01"));

        //遍历消息队列
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            //从队列中取到了数据
            if (!consumerRecords.isEmpty()){
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
                while (recordIterator.hasNext()){
                    //获取一个消费消息
                    ConsumerRecord<String, String> record = recordIterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();
                    System.out.println(topic + "\t" + partition + "\t" + offset + "\t" + key + "\t" + value + "\t" + timestamp);
                }
            }
        }
    }

}
