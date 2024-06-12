import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Vector;
import java.util.Arrays;
import java.io.*;
import java.util.*;
import java.util.Timer;
import java.util.TimerTask;



public class Consumer2 {

    public static void wait(int ms){
        try{
            Thread.sleep(ms);
        }
        catch(InterruptedException ex){
            Thread.currentThread().interrupt();
        }
    }

    public static void file_read(Vector<String> topics,Vector<String> num_of_msg,int[] breakwhileloop,Map<String,KafkaConsumer<String,String>> mp,Vector<String> Group){
        try{ 
            
            System.out.println("Checking priority"); 
            File file=new File("priority.txt");    //creates a new file instance  
            FileReader fr=new FileReader(file);   //reads the file  
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
            topics.clear();
            num_of_msg.clear();
            Group.clear();  
            String line;  
            int flag=-1;
            while((line=br.readLine())!=null){ 
                if(flag==-1){
                    int aa=Integer.parseInt(line);
                    breakwhileloop[0]=aa;
                    flag=0;
                }
                
                else if(flag==0){
                   topics.add(line); //appends line to string buffer 
                   flag=1;
                   String line1=br.readLine();
                   System.out.println(line+" "+line1);
                   Group.add(line1);
                   
                   flag=1;
                }     
                else{
                    num_of_msg.add(line);
                    flag=0;
                }
            } 
            Map <String,String> cp= new  HashMap<String,String>();

            for(int i=0;i<topics.size();i++){
                cp.put(topics.get(i),Group.get(i));
            }

            for (String top: mp.keySet()){
                if(!cp.containsKey(top)){
                    mp.get(top).close();
                }
            }
            
            for (String top: cp.keySet()){
                if(!mp.containsKey(top)){
                    mp.put(top,Server.func("1",cp.get(top)));
                }
            }

            fr.close(); 
            //System.out.println(breakwhileloop);
               //closes the stream and release the resources  
            
        }  
        catch(IOException e){  
            e.printStackTrace();  
        } 
     
    }  

    public static void main(String[] args){
        //declare consumer
        //Initially Subscribe to topic1
        int MINUTES = 1; // The delay in minute
        int[] breakwhileloop={1};
        Vector<String> topics=new Vector<String>();
        Vector<String> Group=new Vector<String>();
        Vector<String> num_of_msg= new Vector<String>();
        Map<String,KafkaConsumer<String,String>> mp= new HashMap<String,KafkaConsumer<String,String>>();
        file_read(topics,num_of_msg,breakwhileloop,mp,Group);
        long start =System.currentTimeMillis();
        long next=MINUTES*1000*60;


      /* Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() { // Function runs every MINUTES minutes.
            file_read(topics,num_of_msg,breakwhileloop,mp,Group);   
             // If the function you wanted was static
            }
        }, 0, 1000 * 60 * MINUTES);*/
        Logger logger=LoggerFactory.getLogger(Consumer.class);
        
        loop:
        while(true){
            
            if(breakwhileloop[0]==0){
                System.out.println("Closing Consumers");
                for(int i=0;i<topics.size();i++){
                if(mp.containsKey(topics.get(i)))
                    mp.get(topics.get(i)).close();
                }
                System.out.println("exiting program");
                System.exit(0) ;
            }

            for(int i=0;i<topics.size();i++){
                    long cur=System.currentTimeMillis();
                    
                    if(cur>=next){
                        file_read(topics,num_of_msg,breakwhileloop,mp,Group);
                        next=cur+MINUTES*1000*60;
                    }

                    if(mp.size()==0){
                        for(int j=0;j<topics.size();j++){
                            mp.put(topics.get(j),Server.func("1",Group.get(j)));
                        }
                    }
                    
                    mp.get(topics.get(i)).subscribe(Arrays.asList(topics.get(i)));
                    System.out.println("Enter topic:"+topics.get(i));
                    int cn=0;
                    int tmp=cn;

                    for(int j=0;j<Integer.parseInt(num_of_msg.get(i));j++){
                        
                        ConsumerRecords<String,String> record= mp.get(topics.get(i)).poll(Duration.ofMillis(200));
                        
                        for(ConsumerRecord<String,String> records: record){
                            logger.info("Successfully received the details as: \n" +"Topic:" + records.topic() + "\n" +
                            "Partition:" + records.partition() + "\n" +"Offset" + records.offset() + "\n" +
                            "Key:" + records.key() + "\n" +"Value:"+records.value());
                            //System.out.printf("Successfully received the details as: \n" +"Topic:" + records.topic() + "\n" +"Value:"+records.value()+"\n");
                            System.out.println("In topic:"+records.topic()+" "+ " Offset " + records.offset()+  "Value:"+records.value());
                            cn++;
                            wait(2000);
                        }
                    }


                    if(breakwhileloop[0]==0){
                        System.out.println("Closing Consumers");
                        for(int ii=0;ii<topics.size();ii++){
                        if(mp.containsKey(topics.get(i)))
                            mp.get(topics.get(ii)).close();
                        }
                        System.out.println("exiting program");
                        System.exit(0) ;
                    }

                    if(tmp!=cn){
                        continue loop;
                        
                    }

            }
        }
        
        
    }
}


    


