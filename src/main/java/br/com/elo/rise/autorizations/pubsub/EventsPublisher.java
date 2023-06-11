package br.com.elo.rise.autorizations.pubsub;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class EventsPublisher {

	private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String IN_TOPIC = "channel-in";

    private KafkaProducer<byte[],byte[]> producer;

    // Publica a mensagem no tópico Kafka
	public void send(byte[] message) {
		producer().send(new ProducerRecord<byte[],byte[]>(IN_TOPIC, message));
	}

	private KafkaProducer<byte[], byte[]> producer() {
		if (this.producer == null) {
	        // Configurações do produtor
	        Properties properties = new Properties();
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
	        this.producer = new KafkaProducer<>(properties);
		}
		return this.producer;
	}
}
