package com.zyp.mysql_offset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/7 19:23
 */
public class ConsumerOffset2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //Kafka集群
        properties.put("bootstrap.servers","ip:port");	//填写自己的ip地址:和端口port
        //消费者组，只要group.id相同，就属于同一个消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //键值的反序列化方式
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //创建kafka消费者
        Consumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //消费者订阅的主题
        consumer.subscribe(Collections.singletonList("first"), new ConsumerRebalanceListener() {
            //该方法会在Rebalance之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                for(TopicPartition topicPartition:collection){
                    String topic = topicPartition.topic();
                    int partition = topicPartition.partition();
                    // 对应分区的偏移量
                    long offset = consumer.position(topicPartition);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String saveDate = simpleDateFormat.format(new Date());
                    Offset data = new Offset(topic,partition,offset,saveDate);
                    commitOffset(data);
                }
            }
            //该方法会在Rebalance之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                for(TopicPartition topicPartition:collection){
                    String topic = topicPartition.topic();
                    int partition = topicPartition.partition();
                    Offset offset = new Offset();
                    offset.setSubject(topic);
                    offset.setPartition(partition);
                    long offset1 = getOffset(offset);
                    //让消费者从指定的主题、分区和位值开始消费
                    if (offset1==-1){
                        continue;
                    }
                    consumer.seek(topicPartition,offset1+1);
                }
            }
        });
        while(true){
            //消费者拉取数据
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            List<Offset> list = new ArrayList<>();
            //输出数据的详细信息
            for (ConsumerRecord<String,String> consumerRecord:consumerRecords) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                        consumerRecord.partition(),consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String saveDate = simpleDateFormat.format(new Date());
                Offset offset = new Offset(consumerRecord.topic(),consumerRecord.partition(),consumerRecord.offset(),saveDate);
                list.add(offset);
            }
            for (Offset offset:list){
                String subject = offset.getSubject();
                if (subject==null){
                    continue;
                }
                Integer partition = offset.getPartition();
                if(partition==null){
                    continue;
                }
                Long offset1 = offset.getOffset();
                if (offset1==null){
                    continue;
                }
                //commitOffset(offset);
            }
        }
    }
    //获取某分区的最新offset
    private static long getOffset(Offset offset){
        Connection connect = DBUtils.connect();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connect.prepareStatement("select `offset` from `offset` where `subject` = ? and `partition` = ? order by save_date desc limit ?,?");
            preparedStatement.setString(1,offset.getSubject());
            preparedStatement.setInt(2,offset.getPartition());
            preparedStatement.setInt(3,0);
            preparedStatement.setInt(4,1);
            resultSet = preparedStatement.executeQuery();
            List<Long> list = new ArrayList<>();
            while (resultSet.next()){
                long offset1 = resultSet.getLong("offset");
                list.add(offset1);
            }
            if (list.size()>0){
                return list.get(0);
            }
            return -1;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }finally {
            DBUtils.close(connect,preparedStatement,resultSet);
        }
    }
    //保存该消费者所有分区的offset
    private static void commitOffset(Offset offset){
        Connection connect = DBUtils.connect();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connect.prepareStatement("insert into offset(`subject`,`partition`,`offset`,`save_date`) values (?,?,?,?)");
            preparedStatement.setString(1,offset.getSubject());
            preparedStatement.setInt(2,offset.getPartition());
            preparedStatement.setLong(3,offset.getOffset());
            preparedStatement.setString(4,offset.getSaveDate());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBUtils.close(connect,preparedStatement,null);
        }

    }
}
