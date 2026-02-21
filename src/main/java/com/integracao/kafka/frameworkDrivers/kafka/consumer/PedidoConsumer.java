package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberPedidoUseCase;
import com.integracao.kafka.domain.entity.Evento;
import com.integracao.kafka.domain.entity.FalhaProcessamento.TipoFalha;
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
    private final GerenciarFalhasUseCase gerenciarFalhasUseCase;

    @KafkaListener(
        topics = "${integrador.topico.entrada-pedido:entrada.pedido}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-pedidos"
    )
    public void consumirPedidoEntrada(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, false);
    }

    @KafkaListener(
        topics = "${integrador.topico.erro-pedido:erro.pedido}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-pedidos-reprocessamento"
    )
    public void reprocessarPedidoComErro(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, true);
    }

    private void processar(ConsumerRecord<String, Evento> record, Acknowledgment ack, boolean origemErro) {
        String topico = record.topic();
        long offset = record.offset();
        int partition = record.partition();

        log.info("[CONSUMER-PEDIDO] Mensagem recebida | topico={} particao={} offset={} origemErro={}",
            topico, partition, offset, origemErro);

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

            receberPedidoUseCase.registrar(pedido);

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
            gerenciarFalhasUseCase.registrarFalha(
                TipoFalha.PEDIDO,
                record.value(),
                ex.getMessage(),
                topico,
                partition,
                offset
            );

            log.warn("[CONSUMER-PEDIDO] Pedido inválido descartado | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            if (origemErro) {
                gerenciarFalhasUseCase.registrarFalha(
                    TipoFalha.PEDIDO,
                    record.value(),
                    "Falha técnica após envio ao tópico de erro: " + ex.getMessage(),
                    topico,
                    partition,
                    offset
                );

                log.error("[CONSUMER-PEDIDO] Reprocessamento falhou e foi armazenado para controle manual | topico={} offset={} erro={}",
                    topico, offset, ex.getMessage());
                ack.acknowledge();
                return;
            }

            log.error("[CONSUMER-PEDIDO] Falha ao processar pedido | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
            throw new RuntimeException("Falha transitória no processamento de pedido", ex);
        }
    }
    
}
