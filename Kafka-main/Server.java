
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;


public class Server {
    public static KafkaConsumer<String,String> func(String poll_time,String gid){
        
        String ipAddress = "localhost";
        String port = "9092";
        //initiate properties
        Properties properties =new Properties();
        properties.put("bootstrap.servers", ipAddress.concat(":").concat(port));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, gid);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,poll_time);
        KafkaConsumer<String,String> kafkaconsumer=new KafkaConsumer<String,String>(properties);
        return kafkaconsumer;

    }
}
