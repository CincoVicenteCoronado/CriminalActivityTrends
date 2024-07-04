package org.mdp.kafka.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;

public class BurstDetector {
    public static final String[] CRIME_SUBSTRINGS = new String[] { "sex abuse", "theft" };
    public static final int FIFO_SIZE = 3;
    public static final int EVENT_START_TIME_INTERVAL = 1 * 60 * 60 * 1000; // convertir a milisegundos
    public static final int EVENT_END_TIME_INTERVAL = 6 * 60 * 60 * 1000; // convertir a milisegundos


    public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length!=2){
            System.err.println("Usage [inputTopic] [blocks]");
            return;
        }
        String[] ZONE_SUBSTRINGS = args[1].split(",");
        for (int i = 0; i < ZONE_SUBSTRINGS.length; i++) {
                 ZONE_SUBSTRINGS[i] = ZONE_SUBSTRINGS[i].toLowerCase();
             }

        Properties props = KafkaConstants.PROPS;

        // randomise consumer ID so messages cannot be read by another consumer
        //   (or at least it's more likely that a meteor wipes out life on Earth)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        // Initialize FIFO
        LinkedList<ConsumerRecord<String, String>> fifo = new LinkedList<ConsumerRecord<String, String>>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // initialize state variables
        boolean inEvent = false;
        int events = 0;

        consumer.subscribe(Arrays.asList(args[0]));

        try{
            while (true) {
                // every ten milliseconds get all records in a batch
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000*60));

                // for all records in the batch
                for (ConsumerRecord<String, String> record : records) {
                    String lowercase = record.value().toLowerCase();

                    boolean breaking = false;
                    for(String cr: CRIME_SUBSTRINGS){
                        if(lowercase.contains(cr)){
            				if(breaking) {
								break;
							}
                        	for(String zs: ZONE_SUBSTRINGS) {
                        		// if so print it out to the console
                        		if(lowercase.contains(zs)) {
                        			breaking = true;
                        			fifo.add(record);
                        			// prevents multiple prints of the same crime with multiple keywords
                        			if(fifo.size() >= FIFO_SIZE){
                        				ConsumerRecord<String, String> oldest = fifo.removeFirst();
                        				long gap = record.timestamp() - oldest.timestamp();

                        				if (gap <= EVENT_START_TIME_INTERVAL && !inEvent) {
                        					System.out.println("START event-id: " + events + ": start: " + oldest.timestamp() + "value: " + oldest.value() +  " rate: " + FIFO_SIZE + " records in " + gap + " ms");
                        					inEvent = true;
                        					events++;
                        				} else if (gap >= EVENT_END_TIME_INTERVAL && inEvent) {
                        					System.out.println("END event-id: " + events + " rate: " + FIFO_SIZE + " records in " + gap + " ms");
                        					inEvent = false;
                        				}
                        			}
                        			break;
                        		}
                        	}
                        }
                    }
                }
            }
        } finally{
            consumer.close();
        }
    }
}
