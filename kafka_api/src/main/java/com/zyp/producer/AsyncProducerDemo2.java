package com.zyp.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/10/27 15:03
 */
//带回调函数的API
//注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试
public class AsyncProducerDemo2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群，bootstrap.servers(*重要*)
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //ack应答机制(*重要*)
        properties.put("acks","all");
        //重试次数
        properties.put("retries",1);
        //批次大小
        properties.put("batch.size",16384);
        //等待时间
        properties.put("linger.ms",1);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory",33554432);
        //键和值的序列化方式(*重要*)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i<10;i++){
            //把每条数据封装成一个ProducerRecord对象
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i));
            producer.send(producerRecord, new Callback() {
                //回调函数，该方法会在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if(exception==null){
                        System.out.println("success->"+recordMetadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }
}
