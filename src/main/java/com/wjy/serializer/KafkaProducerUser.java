package com.wjy.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月03日 15:37:00
 */
public class KafkaProducerUser {

    public static void main(String[] args) {
        //创建kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "wangjunyao100.com:9092,wangjunyao101.com:9092,wangjunyao102.com:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //自定义序列化
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserDefineSerializer.class.getName());

        KafkaProducer<String, User> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 5; i++) {
            User user = new User(i, i + "" + i, new Date());
            ProducerRecord<String, User> record = new ProducerRecord<String, User>("topic01", "key" + i, user);
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }

}
