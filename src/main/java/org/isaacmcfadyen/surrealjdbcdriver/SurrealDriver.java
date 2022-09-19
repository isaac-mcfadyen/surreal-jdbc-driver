package org.isaacmcfadyen.surrealjdbcdriver;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.sql.*;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

public class SurrealDriver implements Driver {

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // Format: ssurreal://host:port/namespace
        String protocol;
        if(url.startsWith("ssurreal://")) {
            protocol = "wss://";
        } else if(url.startsWith("surreal://")) {
            protocol = "ws://";
        } else {
            throw new SQLException("Invalid connection protocol.");
        }
        String namespace = url.split("/")[3];
        String host = url.split("/")[2].split(":")[0];

        String newUrl;
        int port;
        if(url.split("/")[2].split(":").length > 1) {
            port = Integer.parseInt(url.split("/")[2].split(":")[1]);
            newUrl = protocol + host + ":" + port + "/rpc";
        } else {
            newUrl = protocol + host + "/rpc";
        }

        System.out.println("Connecting to " + newUrl + " with namespace " + namespace);

        // Open connection.
        WebsocketConnection connection = new WebsocketConnection(URI.create(newUrl));
        try {
            connection.connectBlocking();
        } catch (InterruptedException e) {
            throw new SQLException("Connection timed out.");
        }

        // Schedule keep alive pings.
        Timer keepaliveTimer = new Timer();
        keepaliveTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                connection.sendPing();
            }
        }, 5000, 5000);

        String username = info.getProperty("user");
        String password = info.getProperty("password");
        JSONObject request = new JSONObject();
        request.put("user", username);
        request.put("pass", password);
        connection.sendMessage("signin", new JSONArray().put(request));

        // Send a message to switch to the requested namespace (default is used before user switches).
        JSONArray useRequest = new JSONArray();
        useRequest.put(namespace);
        useRequest.put("default");
        String useId = connection.sendMessage("use", useRequest);
        connection.awaitResult(useId);

        return new SurrealConnection(connection, keepaliveTimer, namespace);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return (url.startsWith("ws://") || url.startsWith("wss://")) && url.endsWith("/rpc");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    static {
        try {
            DriverManager.registerDriver(new SurrealDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
