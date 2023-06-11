package br.com.elo.rise.autorizations.pubsub;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

public class EventsSubscriber {

	private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String OUT_TOPIC = "channel-out";

	private KafkaReceiver<byte[], byte[]> receiver;
	private Consumer<byte[]> consumer;

	public EventsSubscriber() {

		receiver()
			.receive()
			.map(ReceiverRecord::value)
			.subscribe(message -> {
				if (this.consumer != null) {
					System.out.println("RECEBIDO DO KAFKA");
					this.consumer.accept(message);
				}
			});
	}

	// Logica de escrita
	public void onReceive(Consumer<byte[]> consumer) {
		this.consumer = consumer;
	}

	private KafkaReceiver<byte[], byte[]> receiver() {

		if (this.receiver == null) {
			// Lógica de consumo do Kafka
			Map<String, Object> consumerProps = new HashMap<>();
			consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
			consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "channel-out");
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

			ReceiverOptions<byte[], byte[]> options = ReceiverOptions.create(consumerProps);
			ReceiverOptions<byte[], byte[]> receiverOptions = options.subscription(Collections.singleton(OUT_TOPIC))
					.commitInterval(Duration.ZERO);

			this.receiver = KafkaReceiver.create(receiverOptions);
		}
		return receiver;
	}

}
