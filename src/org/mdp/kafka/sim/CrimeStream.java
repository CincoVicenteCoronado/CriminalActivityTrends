package org.mdp.kafka.sim;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CrimeStream implements Runnable {
	// Formato de fecha para analizar las fechas de crimen
	public final SimpleDateFormat CRIME_DATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	BufferedReader br;
	long startSim = 0;
	long startData = 0;
	long lastData = 0;
	int speedup;
	int id;
	int dateColumnIndex;
	Producer<String, String> producer;
	String topic;

	// Constructor para iniciar la simulación
	public CrimeStream(BufferedReader br, int id, int dateColumnIndex, Producer<String, String> producer, String topic, int speedup){
		this(br,id, dateColumnIndex ,System.currentTimeMillis(),producer,topic,speedup);
	}

	// Constructor que acepta un tiempo de inicio específico para la simulación
	public CrimeStream(BufferedReader br, int id, int dateColumnIndex, long startSim, Producer<String, String> producer, String topic, int speedup){
		this.br = br;
		this.id = id;
		this.dateColumnIndex = dateColumnIndex;
		this.startSim = startSim;
		this.producer = producer;
		this.speedup = speedup;
		this.topic = topic;
	}

	@Override
	public void run() {
		String line;
		long wait = 0;
		try{
			while((line = br.readLine())!=null){
				String[] tabs = line.split("\t");
				if(tabs.length>id && tabs.length>dateColumnIndex ){
					try{
						long timeData = getUnixTime(tabs[dateColumnIndex]);
						if(startData == 0) // first element read
							startData = timeData;
						
						wait = calculateWait(timeData);
						
						String idStr = tabs[id];
						
						if(wait>0){
							Thread.sleep(wait);
						}
						// Enviar el registro al topic de Kafka
						producer.send(new ProducerRecord<String,String>(topic, 0, timeData, idStr, line));
					} catch(ParseException | NumberFormatException pe){
						System.err.println("Cannot parse date "+tabs[dateColumnIndex]);
					}
				}
				
				if (Thread.interrupted()) {
				    throw new InterruptedException();
				}
			}
		} catch(IOException ioe){
			System.err.println(ioe.getMessage());
		} catch(InterruptedException ie){
			System.err.println("Interrupted "+ie.getMessage());
		}
		
		System.err.println("Finished! Messages were "+wait+" ms from target speed-up times.");
	}


	// Método para calcular el tiempo de espera antes de enviar un nuevo registro
	private long calculateWait(long time) {
		long current = System.currentTimeMillis();
		

		long delaySim = current - startSim;
		if(delaySim<0){
			return delaySim*-1;
		}
		

		long delayData = time - startData;
		long shouldDelay = delayData / speedup;
		

		if(delaySim>=shouldDelay) return 0;

		else return shouldDelay - delaySim;
	}

	// Método para convertir una cadena de fecha y hora en tiempo UNIX
	public long getUnixTime(String dateTime) throws ParseException{
		Date d = CRIME_DATE.parse( dateTime );
		return d.getTime();
	}
}