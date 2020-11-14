package com.zyp.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/10 18:09
 */
public class ProducerInterceptor {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群，bootstrap.servers
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //ack应答机制
        properties.put("acks", "all");
        //重试次数
        properties.put("retries", "3");
        //批次大小
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator缓存区大小
        properties.put("buffer.memory", 335544323);
        //键值的序列化方式
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //构建拦截器链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.zyp.interceptor.TimeInterceptor");
        interceptors.add("com.zyp.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        //创建生产者，发送数据
        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            //把每条数据封装成一个ProducerRecord对象
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first", " 第"+Integer.toString(i)+"条 ");
            //发送消息
            producer.send(producerRecord);

            System.out.println("has sent msg [" + i + "]");
        }
        //关闭生产者
        producer.close();
    }
}
