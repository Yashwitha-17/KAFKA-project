import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class SampleProducer {

    public SampleProducer(String topic,int tim,int k1,int k2) {

        // constructing the message that is needed to be sent
        String topicName = topic;
        String ipAddress = "localhost";
        String port = "9092";

        //initiate properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ipAddress.concat(":").concat(port));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        for(int i=k1;i<k2;i++){
            
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>(topicName, "message from :"+topic+"  message_no:"+Integer.toString(i));
            // creating the instance of kafka producer
           //  kafkaProducer.send(producerRecord);
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    Logger logger= LoggerFactory.getLogger(SampleProducer.class);
                    if (e== null) {
                        logger.info("Successfully received the details as: \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                                System.out.printf("Successfully received the details as: \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n"
                                );
                    }

                    else {
                        logger.error("Can't produce,getting error",e);

                    }
                }
            });
            kafkaProducer.close();
            wait(tim);

        }

    }


    public static void wait(int ms){
        try{
            Thread.sleep(ms);
        }

        catch(InterruptedException ex){
            Thread.currentThread().interrupt();
        }
    }
}