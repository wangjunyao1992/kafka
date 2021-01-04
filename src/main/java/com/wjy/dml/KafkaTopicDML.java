package com.wjy.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 必须将 wangjunyao100.com:9092,wangjunyao101.com:9092,wangjunyao102.com:9092
 * 这三个主机的IP和主机名映射配置在操作系统中  hosts
 * @author wangjunyao
 * @version 1.0.0
 * @Description TODO
 * @createTime 2021年01月03日 14:36:00
 */
public class KafkaTopicDML {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //创建KafkaAdminClient
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "wangjunyao100.com:9092,wangjunyao101.com:9092,wangjunyao102.com:9092");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

        //创建Topic   异步创建
//        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic("topic03", 3, (short) 3)));
//        createTopicsResult.all().get();//同步创建

        //查看Topic列表
        ListTopicsResult listTopics = adminClient.listTopics();
        Set<String> topicNames = listTopics.names().get();
        for (String topicName : topicNames) {
            System.out.println(topicName);
        }

        //删除
//        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic03", "topic04"));
//        deleteTopicsResult.all().get();//同步删除

        //查看Topic详细信息
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry : stringTopicDescriptionMap.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }

        //关闭AdminClient
        adminClient.close();
    }

}
