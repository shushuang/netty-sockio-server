package com.corundumstudio.socketio.demo;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.corundumstudio.socketio.*;
import com.corundumstudio.socketio.listener.ConnectListener;
import com.corundumstudio.socketio.listener.DataListener;
import com.corundumstudio.socketio.listener.DisconnectListener;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 版本更新
 * 1. 增加心跳机制 初步版-检测连接丢失， 高级版本-触发服务器推送消息
 *  redis引入
 * 2. 消息按序编号
 * 3. 会话实现
 *
 */


public class TestChatLauncher {

    private static Logger logger = LoggerFactory.getLogger(TestChatLauncher.class);

    private volatile static int pass5k = 0;

    public static void main(String[] args) throws InterruptedException {

        Configuration config = new Configuration();
        config.setHostname("0.0.0.0");
        config.setPort(9092);

        // 是否需要引入会话概念，新连接用户创建一个会话，用户和用户之间的会话，
        // 重新连接，可以复用之前的会话，获取消息，会话中消息的顺序是一致的
        // userName, toUserName, 可以对应一个会话。
        // 会话中消息顺序以到达服务器时间排序，
        // 问题一：如果到达不同的接入服务器，id编号，并
        // 不能保证是有序的。需要提供一个序号策略，会话中每一句话会有虚拟序号，
        // 问题二：群消息顺序问题
        // 解决方案： 如果收到消息后，都扔到同一个redis队列，是否会不存在这个问题。
        // 问题：如果是群聊天需要拉取消息记录。如何确定那些消息需要投递给客户端

        final SocketIOServer server = new SocketIOServer(config);
        final Map<String, SocketIOClient> routeMap =
                Maps.newConcurrentMap();

        AtomicLong num = new AtomicLong(0);
        // 1-5

        MqManager mqManager = MqManager.newInstance()
                .sendTopic("PushTopic2")
                .consumeTopic("PushTopic1")
                .producerGroup("ProducerGroup1")
                .consumerGroup("ConsumerGroup1");
        mqManager.registerConsumeListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
//                    System.out.println(new String(msg.getBody()));
                    if(CollectionUtils.isEmpty(server.getAllClients())){
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    String[] tokens = new String(msg.getBody()).split(":");
                    SocketIOClient socketIOClient = routeMap.get(tokens[0]);
                    if(socketIOClient == null){
                        System.out.println("no user" + tokens[0]);
                    } else {
                        ChatObject data = new ChatObject(tokens[0], tokens[1]);
                        socketIOClient.sendEvent("ackevent3", new VoidAckCallback() {
                            protected void onSuccess() {
//                                System.out.println("void ack from: " + socketIOClient.getSessionId());
                            }
                        }, data);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        try {
            mqManager.start();
        }catch (MQClientException e){
            try {
                logger.error("mqstart error", e);
                mqManager.destroy();
            }catch (MQClientException e2){
                logger.error("mq destroy error", e2);
            }
        }


        server.addConnectListener(new ConnectListener() {
            @Override
            public void onConnect(SocketIOClient client) {
                // 分配一个编号。建立client和编号的双向对应关系。
                Long current = num.incrementAndGet();
                client.set("key", current.toString());
                routeMap.put(current.toString(), client);
//                client.sendEvent("regevent");
            }
        });
//        server.addEventListener("regackevent", ChatObject.class, new DataListener<ChatObject>() {
//            @Override
//            public void onData(SocketIOClient client, ChatObject data, AckRequest ackSender) throws Exception {
//                if(ackSender.isAckRequested()){
//                    ackSender.sendAckData(data.getUserName(), "regackevent" );
//                }
//                routeMap.put(data.getUserName(), client);
//            }
//        });

        server.addDisconnectListener(new DisconnectListener() {
            @Override
            public void onDisconnect(SocketIOClient client) {
                routeMap.remove(client);
            }
        });
        //  一个全局的routeCacheManager
        server.addEventListener("usersevent", ChatObject.class, new DataListener<ChatObject>() {
            @Override
            public void onData(final SocketIOClient client, ChatObject data, AckRequest ackSender) throws Exception {
                if(ackSender.isAckRequested()){
                    ackSender.sendAckData("client message was delivered to server!", "yeah!");
                }
                // 服务器再发送回应消息给浏览器，浏览器需要给予回应
                ChatObject responseObject = new ChatObject("server", routeMap.keySet().toString());
                client.sendEvent("ackevent3", new VoidAckCallback() {
                    protected void onSuccess() {
                        System.out.println("void ack from: " + client.getSessionId());
                    }
                }, responseObject);
            }
        });


        server.addEventListener("ackevent1", ChatObject.class, new DataListener<ChatObject>() {
            @Override
            public void onData(final SocketIOClient client, ChatObject data, final AckRequest ackRequest) {
                // check is ack requested by client,
                // but itw's not required check
//                if (ackRequest.isAckRequested()) {
//                    // send ack response with data to client
//                    ackRequest.sendAckData("client message was delivered to server!", "yeah!");
//                }
                ChatObject send = new ChatObject();
                String msg = client.get("key") + ":" + data.getMessage();
                // 这里相当于普通消息发送，只接受一个服务器是否收到消息的回馈
                // 如果发送给另外用户的消息，则服务器进行转发
                mqManager.sendMessage(msg);
//                String toUserName = data.getToUserName();
//                if(toUserName != null){
//                    if(routeMap.get(toUserName)!=null){
//                        SocketIOClient toClient = routeMap.get(toUserName);
//                        toClient.sendEvent("ackevent3", new VoidAckCallback() {
//                            protected void onSuccess() {
//                                System.out.println("void ack from: " + client.getSessionId());
//                            }
//                        }, data);
//                    }
//                }
            }
        });

        server.start();





        Thread.sleep(Integer.MAX_VALUE);

        server.stop();
    }

}
