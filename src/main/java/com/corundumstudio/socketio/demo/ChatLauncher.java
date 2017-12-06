package com.corundumstudio.socketio.demo;

import com.corundumstudio.socketio.listener.*;
import com.corundumstudio.socketio.*;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ChatLauncher {

    private static Logger log = LoggerFactory.getLogger(ChatLauncher.class);

    public static void main(String[] args) throws InterruptedException {

        Configuration config = new Configuration();
        config.setHostname("0.0.0.0");
        config.setPort(9091);

        final Map<SocketIOClient, Long> socketTimeMap = Maps.newConcurrentMap();

        // 存储客户端编号。 1-50
        // 发送目标也是同样的编号。 客户端2接收到信息后，回复一条信息。一个sender，一个consumer。
        // consumer收到消息后，根据编号，发送消息给指定客户端。

        final SocketIOServer server = new SocketIOServer(config);
        server.addEventListener("chatevent", ChatObject.class, new DataListener<ChatObject>() {
            @Override
            public void onData(SocketIOClient client, ChatObject data, AckRequest ackRequest) {
//                 broadcast messages to all clients
                //server.getBroadcastOperations().sendEvent("chatevent", data)
                socketTimeMap.put(client, System.currentTimeMillis());
                client.sendEvent("ackevent2", new VoidAckCallback() {
                            protected void onSuccess() {
//                                System.out.println("void ack from: " + client.getSessionId());
                            }
                        }
                );//                                System.out.println("void ack from: " + client.getSessionId());

            }
        });

        server.addEventListener("ackevent3", ChatObject.class, new DataListener<ChatObject>() {
            @Override
            public void onData(SocketIOClient client, ChatObject chatObject, AckRequest ackRequest) throws Exception {
                long current = System.currentTimeMillis();
                if(current % 10 == 0) {
                    log.error("time:" + String.valueOf(current - socketTimeMap.get(client)));
                }
            }
        });



        server.start();

        Thread.sleep(Integer.MAX_VALUE);

        server.stop();
    }

}
