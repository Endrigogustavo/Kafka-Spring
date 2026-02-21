package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.useCase.subscribe.ReceberPedidoUseCase;
import com.integracao.kafka.domain.entity.Evento;
import com.integracao.kafka.domain.entity.Pedido;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoConsumer {

    private final PublicarEventoPort publicarEventoPort;
    private final ObjectMapper objectMapper;
    private final ReceberPedidoUseCase receberPedidoUseCase;

    @KafkaListener(
        topics = "${integrador.topico.entrada-pedido:entrada.pedido}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-pedidos"
    )
    public void consumirPedidoEntrada(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        String topico = record.topic();
        long offset = record.offset();
        int partition = record.partition();

        log.info("[CONSUMER-PEDIDO] Mensagem recebida | topico={} particao={} offset={}", topico, partition, offset);

        try {
            Evento eventoEntrada = record.value();
            Pedido pedido = objectMapper.convertValue(eventoEntrada.getPayload(), Pedido.class);

            if (pedido.getNumeroPedido() == null || pedido.getNumeroPedido().isBlank()) {
                throw new IllegalArgumentException("Pedido sem numeroPedido no payload");
            }

            // Enriquece pedido com metadados de processamento Kafka
            pedido.setDataProcessamento(java.time.LocalDateTime.now());
            pedido.setStatusProcessamento("PROCESSADO");
            pedido.setKafkaOffset(offset);
            pedido.setKafkaPartition(partition);
            pedido.setKafkaTopic(topico);
            pedido.setKafkaTimestamp(java.time.Instant.ofEpochMilli(record.timestamp()));

            log.info("[CONSUMER-PEDIDO] Pedido enriquecido | numero={} offset={} partition={}",
                pedido.getNumeroPedido(), offset, partition);

            // Cria evento de saída e publica
            Evento eventoSaida = Evento.builder()
                .tipo("PEDIDO_PROCESSADO")
                .origem("CONSUMER_KAFKA")
                .destino("SISTEMA_PEDIDOS")
                .payload(pedido)
                .status(Evento.StatusEvento.ENVIADO)
                .build();

            publicarEventoPort.publicar("saida.pedido", eventoSaida);
            ack.acknowledge();

            log.info("[CONSUMER-PEDIDO] Pedido publicado em saida.pedido | numero={} eventoId={}",
                pedido.getNumeroPedido(), eventoSaida.getId());

        } catch (IllegalArgumentException ex) {
            log.warn("[CONSUMER-PEDIDO] Pedido inválido descartado | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[CONSUMER-PEDIDO] Falha ao processar pedido | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
        }
    }
    
}
