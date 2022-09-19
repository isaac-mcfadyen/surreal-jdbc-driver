package org.isaacmcfadyen.surrealjdbcdriver;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.util.HashMap;
import java.util.UUID;

public class WebsocketConnection extends WebSocketClient {
    private HashMap<String, JSONObject> responses;
    public WebsocketConnection(URI uri) {
        super(uri);
        responses = new HashMap<>();
    }

    public String sendMessage(String method, JSONArray params) {
        String randomId = UUID.randomUUID().toString();

        JSONObject message = new JSONObject();
        message.put("id", randomId);
        message.put("method", method);
        message.put("params", params);
        System.out.println("Sending request to SurrealDB: " + message.toString());
        this.send(message.toString());
        return randomId;
    }

    public JSONObject awaitResult(String id) {
        synchronized (this) {
            while (!this.responses.containsKey(id)) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            JSONObject response = this.responses.get(id);
            this.responses.remove(id);
            return response;
        }
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
    }

    @Override
    public void onMessage(String s) {
        System.out.println("Message from SurrealDB: " + s);
        JSONObject response = new JSONObject(s);
        String id = response.getString("id");
        synchronized (this) {
            this.responses.put(id, response);
            this.notify();
        }
    }

    @Override
    public void onClose(int i, String s, boolean b) {
    }

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }
}
