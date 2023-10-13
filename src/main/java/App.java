import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Timer;
import java.util.TimerTask;
public class App {

    public static class OnMessageCallback implements MqttCallback{
        @Override
        public void connectionLost(Throwable throwable) {
            System.out.println("Connection lost");
        }

        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
            System.out.println("Message arrived: " + mqttMessage);
            System.out.println("Content");
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(mqttMessage.toString());
            jsonNode.fields().forEachRemaining(entry -> {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            });
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            System.out.println("Delivery complete-------------------------------" + iMqttDeliveryToken.isComplete());
        }
    }
    public static void main(String[] args) {
        String pubTopic = "/iot/device";
        String subTopic = "/iot/device";
        String content  = "{\"id\":11, \"packet_no\":126, \"temperature\":30, \"humidity\":60, \"tds\":1100, \"pH\":5.0}";
        int qos = 2;
        String broker = "tcp://broker.hivemq.com:1883";
        String clientId = "30c019a2-5d1a-446a-a0b9-956e4b422157";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            // Initialize connection
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");

            // Subscribe to topic
            System.out.println("Subscribing to topic: " + subTopic);
            sampleClient.subscribe(subTopic, qos);
            sampleClient.setCallback(new OnMessageCallback());

            // Publish message to topic
            System.out.println("Publishing message: "+ content + " to topic: " + pubTopic);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            sampleClient.publish(pubTopic, message);

            // Delay 1s to receive and display parsed message
            Timer timer = new Timer();
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        sampleClient.disconnect();
                    } catch (MqttException me) {
                        me.printStackTrace();
                    }
                    System.out.println("Disconnected");
                    System.exit(0);
                }
            };
            timer.schedule(task, 1000);
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    }
}
