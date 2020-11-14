package com.zyp.review;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/6 17:00
 */
public class AsyncProducerDemo2_1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //Kafka集群，bootstrap.servers(必须配置)
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //ack应答机制(重要)
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,335544323);
        //键和值的序列化方式(重要)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者，发送数据
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            //把每条数据封装成一个ProducerRecord对象
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first",Integer.toString(i),Integer.toString(i));
            producer.send(producerRecord, new Callback() {
                //回调函数，该方法会在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("主题中分区的偏移量success->"+recordMetadata.offset());
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }
        //关闭生产者,如果不关闭，可能程序运行过快导致sender(满足batch.size或linger.ms才会发送)线程没有发送数据到主题
        producer.close();
    }
}
