package org.apache.iotdb.mqttpipeplugin;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.io.IOException;
import java.util.Random;

public class HelloApplication extends Application {

    public static void main(String[] args) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setHost("172.20.31.76", 1883);
//        mqtt.setUserName("root");
//        mqtt.setPassword("root");
        mqtt.setVersion("3.1.1");
        mqtt.setConnectAttemptsMax(30);
        mqtt.setReconnectDelay(10);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Random random = new Random();
        while (true) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                String payload =
                        String.format(
                                "{\n"
                                        + "\"device\":\"root.sg.devp0\",\n"
                                        + "\"timestamp\":%d,\n"
                                        + "\"measurements\":[\"s0\"],\n"
                                        + "\"values\":[%f]\n"
                                        + "}",
                                System.currentTimeMillis(), random.nextFloat());
                sb.append(payload).append(",");

                // publish a json object
                Thread.sleep(1);
                connection.publish("root/sg/devp0/s0", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
                System.out.println(payload);
            }
            // publish a json array
            sb.insert(0, "[");
            sb.replace(sb.lastIndexOf(","), sb.length(), "]");
            connection.publish("root/sg/devp0/s0", sb.toString().getBytes(), QoS.AT_LEAST_ONCE, false);

        }
//        connection.disconnect();
    }

    @Override
    public void start(Stage primaryStage) throws Exception {

    }
}