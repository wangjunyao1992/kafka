package com.wjy.offsets;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月03日 15:44:00
 */
public class KafkaConsumeOffsets_4 {

    public static void main(String[] args) {
        //创建kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "wangjunyao100.com:9092,wangjunyao101.com:9092,wangjunyao102.com:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "G4");

        //earliest  （第一次访问Kafka服务，没有该消费者组的 偏移量时起作用，一旦有了偏移量，下次就从偏移量开始读取）
        //自动将偏移量重置为最早的偏移量（从topic最早的记录开始读取）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //配置offset自动提交的时间间隔  30s
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 30000);
        //offset偏移量自动提交  设置为false，AUTO_COMMIT_INTERVAL_MS_CONFIG 就没用了
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

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

                    //手动提交消费者偏移量  这里记得要 + 1，提交给Kafka的是下次读取的位置
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            System.out.println("offsets : " + offsets + "\t" + "exception : " + exception);
                        }
                    });
                }
            }
        }
    }

}
