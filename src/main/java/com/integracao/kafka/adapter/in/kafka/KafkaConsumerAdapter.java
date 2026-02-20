package com.integracao.kafka.adapter.in.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.Pedido;
import com.integracao.kafka.port.in.ProcessarEventoPort;
import com.integracao.kafka.port.out.PublicarEventoPort;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerAdapter {

    private final ProcessarEventoPort processarEventoPort;
    private final PublicarEventoPort publicarEventoPort;
    private final ObjectMapper objectMapper;

    /**
     * Consome do tópico de entrada do Sistema A.
     * ACK manual: confirma o offset APENAS após processamento bem-sucedido.
     * Em caso de falha, o Kafka reentrega (at-least-once delivery).
     */
    @KafkaListener(
        topics  = "${integrador.topico.entrada:entrada.evento}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}"
    )
    public void consumir(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        String topico  = record.topic();
        long   offset  = record.offset();
        int    particao = record.partition();

        log.info("[CONSUMER] Mensagem recebida | topico={} particao={} offset={}", topico, particao, offset);

        try {
            Evento evento = record.value();
            processarEventoPort.executar(evento);

            // Confirma offset apenas após sucesso — mensagem não será reentregue
            ack.acknowledge();
            log.info("[CONSUMER] ACK confirmado | topico={} offset={}", topico, offset);

        } catch (IllegalArgumentException ex) {
            // Erro de dados inválidos: não faz sentido retentar — faz ACK e descarta
            log.warn("[CONSUMER] Mensagem inválida descartada | offset={} erro={}", offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            // Erro de infraestrutura: NÃO faz ACK → Kafka reentrega após intervalo
            // Após N tentativas configuradas no DefaultErrorHandler, vai para a DLQ
            log.error("[CONSUMER] Falha ao processar | offset={} erro={} — sem ACK, será reenfileirado", offset, ex.getMessage());
            // Não chama ack.acknowledge() → mensagem volta para a fila
        }
    }

    /**
     * Consome pedidos do tópico de entrada, enriquece com metadados Kafka,
     * e publica no tópico de saída (padrão entrada → processamento → saída)
     */
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
            // Não faz ACK → será reenfileirado
        }
    }
}
