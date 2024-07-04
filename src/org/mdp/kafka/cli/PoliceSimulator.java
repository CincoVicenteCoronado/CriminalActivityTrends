package org.mdp.kafka.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.mdp.kafka.def.KafkaConstants;
import org.mdp.kafka.sim.CrimeStream;

public class PoliceSimulator {
	public static int CRIME_ID = 1;
	public static int DATE_ID = 3;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		if (args.length != 3) {
			System.err.println("Usage: [crime_data_file_gzipped] [crime_topic] [speed_up (int)]");
			return;
		}
		BufferedReader crimeData = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(args[0]))));

		String crimeTopic = args[1];

		int speedUp = Integer.parseInt(args[2]);

		Producer<String, String> crimeProducer = new KafkaProducer<String, String>(KafkaConstants.PROPS);

		CrimeStream crimeStream = new CrimeStream(crimeData,CRIME_ID,  DATE_ID, crimeProducer, crimeTopic, speedUp);

		Thread crimeThread = new Thread(crimeStream);

		crimeThread.start();

		try {
			crimeThread.join();
		} catch (InterruptedException e) {
			System.err.println("Interrupted!");
		}

		crimeProducer.close();
	}
}
