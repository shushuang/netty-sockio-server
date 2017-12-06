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


public class TestChatLauncher2 {

    private static Logger logger = LoggerFactory.getLogger(TestChatLauncher2.class);

    private volatile static int pass5k = 0;

    public static void main(String[] args) throws InterruptedException {

        Configuration config = new Configuration();
        config.setHostname("0.0.0.0");
        config.setPort(9091);

        final Map<String, SocketIOClient> routeMap =
                Maps.newConcurrentMap();

        final SocketIOServer server = new SocketIOServer(config);

        AtomicLong num = new AtomicLong(0);


        MqManager mqManager = MqManager.newInstance()
                .sendTopic("PushTopic1")
                .consumeTopic("PushTopic2")
                .producerGroup("ProducerGroup2")
                .consumerGroup("ConsumerGroup2");


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
                    }}
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
//                routeMap.put(client.getSessionId().toString(), client);
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
//                if(routeMap.size() == 5000 && pass5k == 0){
//                    pass5k = 1;
//                    logger.error("pass 5000");
//                }
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
