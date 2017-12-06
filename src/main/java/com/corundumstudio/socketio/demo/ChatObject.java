package com.corundumstudio.socketio.demo;

import redis.clients.jedis.Jedis;

public class ChatObject {

    private String userName;
    // 发送给哪一个用户
    private String toUserName;

    private String message;

    public ChatObject() {
    }

    public ChatObject(String userName, String message) {
        super();
        this.userName = userName;
        this.message = message;
    }

    public ChatObject(String userName, String toUserName, String message){
        super();
        this.userName = userName;
        this.message = message;
        this.toUserName = toUserName;
    }


    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getToUserName() {return toUserName; }
    public void setToUserName(String toUserName) { this.toUserName = toUserName; }


    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }


    public static void main(String[] args){
        Jedis jedis = new Jedis();
        jedis.set("foo", "bar");
        String bar = jedis.get("foo");
        System.out.println(bar);
    }
}
