package com.zyp.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Map;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/10 17:13
 */
public class TimeInterceptor implements ProducerInterceptor {
    //用户可以在该方法中对消息做任何操作，但最好保证不要修改消息所属的topic和分区，否则会影响目标分区的计算
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        //创建一个新的Record，把时间戳写到消息体的最前面
        return new ProducerRecord(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),
                producerRecord.key(),new Date().getTime()+" "+producerRecord.value());
    }

    //该方法会在消息从RecordAccumulator成功发送到KafkaBroker之后，或者在发送过程中失败时调用
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    //关闭interceptor，主要用于执行一些资源清理工作
    @Override
    public void close() {

    }

    //获取配置信息和初始化数据时调用
    @Override
    public void configure(Map<String, ?> map) {

    }
}
