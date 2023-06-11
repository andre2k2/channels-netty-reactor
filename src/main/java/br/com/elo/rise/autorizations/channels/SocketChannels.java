package br.com.elo.rise.autorizations.channels;

import br.com.elo.rise.autorizations.pubsub.EventsPublisher;
import br.com.elo.rise.autorizations.pubsub.EventsSubscriber;
import reactor.core.publisher.Flux;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

public class SocketChannels {

	private static final byte[] RESPONSE_CODE = new byte[] {0,0};

	public static void main(String[] args) {

    	EventsPublisher publisher = new EventsPublisher();
    	EventsSubscriber subscriber = new EventsSubscriber();

    	DisposableServer server = TcpServer.create()
            .host("localhost")
            .port(8888)
            .observe(new ConnectionObserver() {
				@Override
				public void onStateChange(Connection connection, State newState) {
					System.out.println("Connection state change: " + newState);
				}
            })
            .doOnBound(c -> System.out.println("ISO8583 Server iniciado..."))
            .doOnUnbound(c -> System.out.println("Finalizado."))
            .doOnConnection(conn -> {
            	conn.addHandlerLast(new ISO8583Decoder());
            	conn.addHandlerLast(new ISO8583Encoder());
            })
            .doOnChannelInit((observer, channel, remoteAddress) -> System.out.println("CHANNEL INIT " + channel.id() ))
            .handle((in, out) -> {

            	// Lógica de leitura
                in.receive()
                	.asByteArray()
                    .subscribe(message -> {
                    	publisher.send(message);
                    	out.sendByteArray(Flux.just(RESPONSE_CODE)).then().subscribe();
                    });

                // Logica de escrita
                subscriber.onReceive(message -> {
                	System.out.println("RECEBIDO OUT STREAM");
                	out.sendByteArray(Flux.just(message)).then().subscribe();
                });

                return out.neverComplete();
            })
            .bindNow();

        server.onDispose().block();
    }
}
