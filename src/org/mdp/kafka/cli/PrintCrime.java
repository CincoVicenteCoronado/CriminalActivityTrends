package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class PrintCrime {
	// Palabras clave para detectar crímenes
	public static final String[] CRIME_SUBSTRINGS = new String[] { "sex abuse", "theft" };
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		if(args.length!=2){
			System.err.println("Usage [inputTopic] [blocks]");
			return;
		}

		// Palabras clave de zonas donde se filtran los crímenes
        String[] ZONE_SUBSTRINGS = args[1].split(",");
        for (int i = 0; i < ZONE_SUBSTRINGS.length; i++) {
                 ZONE_SUBSTRINGS[i] = ZONE_SUBSTRINGS[i].toLowerCase();
             }

		
		Properties props = KafkaConstants.PROPS;


		// Generar un ID de grupo aleatorio para el consumidor Kafka
		// Esto asegura que los mensajes no sean leídos por otros consumidores
		// (o al menos reduce la probabilidad de conflicto)

		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Arrays.asList(args[0]));
		
		try{
			
			while (true) {
				// Obtener todos los registros en lotes cada 10 minutos
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000*60));<
				// Procesar todos los registros en el lote
				for (ConsumerRecord<String, String> record : records) {
					
					String lowercase = record.value().toLowerCase();

					//Verificar si el valor del registro contiene alguna palabra clave de crimen


					boolean breaking = false;
					for(String cr: CRIME_SUBSTRINGS){
						// if so print it out to the console
						
						if(lowercase.contains(cr)){

							if(breaking) {
								break;
							}
                        	for(String zs: ZONE_SUBSTRINGS) {
								// Imprimir el valor del registro en la consola
                        		if(lowercase.contains(zs)) {
                        			breaking = true;
                        			System.out.println(record.value());

									// Evitar múltiples impresiones del mismo crimen con múltiples palabras clave
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
