package com.corundumstudio.socketio.demo;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqManager {
    private static Logger logger = LoggerFactory.getLogger(MqManager.class);
    private String sendTopic = "PushTopic";
    private String sendTag = "push";
    private String consumeTopic = "PushTopic";
    private String consumeTag = "push";

    private  DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
    private DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1");

    public MqManager(){

    }

    public static MqManager newInstance(){
        return new MqManager();
    }

    public MqManager producerGroup(String producerGroup){
        this.producer.setProducerGroup(producerGroup);
        return this;
    }

    public MqManager consumerGroup(String consumerGroup){
        this.consumer.setConsumerGroup(consumerGroup);
        return this;
    }

    public MqManager consumeTopic(String topic){
        this.consumeTopic = topic;
        return this;
    }

    public MqManager consumeTag(String tag){
        this.consumeTag = tag;
        return this;
    }

    public MqManager sendTopic(String topic){
        this.sendTopic = topic;
        return this;
    }

    public MqManager sendTag(String tag){
        this.sendTag = tag;
        return this;
    }


    public boolean sendMessage(String message){
        Message msg = new Message(this.sendTopic, this.sendTag, "1", message.getBytes());
        try {
            SendResult result = producer.send(msg);
            SendStatus sendStatus = result.getSendStatus();
            if(sendStatus == SendStatus.SEND_OK){
                return true;
            }
        }catch (Exception e){
            logger.error("send message error", e);
        }
        return false;
    }

    public void registerConsumeListener(MessageListenerConcurrently listenerConcurrently){
        consumer.registerMessageListener(listenerConcurrently);
    }

    public MqManager start() throws MQClientException{
        try {
            producer.setNamesrvAddr("10.81.128.245:9876");
            consumer.setNamesrvAddr("10.81.128.245:9876");
            consumer.subscribe(this.consumeTopic, this.consumeTag);

            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            //wrong time format 2017_0422_221800
            consumer.setConsumeTimestamp("20170422221800");
        }catch (MQClientException e){

        }
        producer.start();
        consumer.start();
        return this;
    }

    public void destroy() throws MQClientException{
        producer.shutdown();
        consumer.shutdown();
    }

}
