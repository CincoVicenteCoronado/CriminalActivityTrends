package org.mdp.kafka.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mdp.kafka.def.KafkaConstants;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class CrimeFilter {
    // Palabras clave para detectar crímenes
    public static final String[] CRIME_SUBSTRINGS = new String[] { "sex abuse", "theft" };

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage [inputTopic] [outputTopic] [blocks]");
            return;
        }

        Properties props = KafkaConstants.PROPS;
        // Palabras clave de zonas donde se filtran los crímenes
        String[] ZONE_SUBSTRINGS = args[2].split(",");
        for (int i = 0; i < ZONE_SUBSTRINGS.length; i++) {
                 ZONE_SUBSTRINGS[i] = ZONE_SUBSTRINGS[i].toLowerCase();
             }

        // Generar un ID de grupo aleatorio para el consumidor Kafka
        // Esto asegura que los mensajes no sean leídos por otros consumidores
        // (o al menos reduce la probabilidad de conflicto)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        consumer.subscribe(Arrays.asList(args[0]));

        try{
        	
            while (true) {
                // every ten milliseconds get all records in a batch
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000*60));
                // for all records in the batch
                for (ConsumerRecord<String, String> record : records) {
      
                    String lowercase = record.value().toLowerCase();

                    // check if record value contains keyword
                    // (could be optimised a lot)
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
                        			// prevents multiple prints of the same crime with multiple keywords
                                    producer.send(new ProducerRecord<String, String>(args[1], 0, record.timestamp(), record.key(), record.value()));
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
