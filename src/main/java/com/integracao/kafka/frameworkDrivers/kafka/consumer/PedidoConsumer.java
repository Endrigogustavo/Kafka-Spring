package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberPedidoUseCase;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;
import com.integracao.kafka.domain.model.Pedido;

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
    private final com.integracao.kafka.application.service.PedidoService pedidoService;

    @Value("${integrador.topico.saida-pedido:integrador.pedido.processado}")
    private String topicoSaidaPedido;

    @Value("${integrador.topico.dlq-pedido:integrador.pedido.dlq}")
    private String topicoDlqPedido;

    @Value("${integrador.topico.retry-pedido:integrador.pedido.retry}")
    private String topicoRetryPedido;

    @Value("${integrador.reprocessamento.max-tentativas:5}")
    private int maxTentativasRetry;

    @KafkaListener(
        topics = "${integrador.topico.entrada-pedido:integrador.pedido.recebido}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-pedidos"
    )
    public void consumirPedidoEntrada(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, false);
    }

    @KafkaListener(
        topics = "${integrador.topico.retry-pedido:integrador.pedido.retry}",
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

            log.info("[CONSUMER-PEDIDO] Enviando pedido para persistencia no banco | numero={} cliente={} produto={} topico={} particao={} offset={}",
                pedido.getNumeroPedido(), pedido.getCliente(), pedido.getProduto(), topico, partition, offset);
            pedidoService.criarPedido(pedido);
            log.info("[CONSUMER-PEDIDO] Persistencia de pedido concluida no banco | numero={} topico={} particao={} offset={}",
                pedido.getNumeroPedido(), topico, partition, offset);

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

            publicarEventoPort.publicar(topicoSaidaPedido, eventoSaida);
            ack.acknowledge();

            log.info("[CONSUMER-PEDIDO] Pedido publicado | topicoSaida={} numero={} eventoId={}",
                topicoSaidaPedido, pedido.getNumeroPedido(), eventoSaida.getId());

        } catch (IllegalArgumentException ex) {
            gerenciarFalhasUseCase.registrarFalha(
                TipoFalha.PEDIDO,
                record.value(),
                ex.getMessage(),
                topico,
                partition,
                offset
            );

            publicarEventoPort.publicar(topicoDlqPedido, record.value());

            log.warn("[CONSUMER-PEDIDO] Pedido inválido enviado para DLQ | topicoOrigem={} topicoDlq={} offset={} erro={}",
                topico, topicoDlqPedido, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            if (origemErro) {
                Evento eventoRetry = record.value();
                int tentativaAtual = obterTentativaAtual(eventoRetry);

                if (tentativaAtual < maxTentativasRetry) {
                    eventoRetry.setTentativasRetry(tentativaAtual + 1);
                    eventoRetry.setStatus(Evento.StatusEvento.FALHA);
                    publicarEventoPort.publicar(topicoRetryPedido, eventoRetry);

                    log.warn("[CONSUMER-PEDIDO] Reprocessamento falhou, reenviado para retry | topicoOrigem={} topicoRetry={} offset={} tentativa={}/{} erro={}",
                        topico, topicoRetryPedido, offset, tentativaAtual + 1, maxTentativasRetry, ex.getMessage());
                    ack.acknowledge();
                    return;
                }

                gerenciarFalhasUseCase.registrarFalha(
                    TipoFalha.PEDIDO,
                    eventoRetry,
                    "Falha técnica após envio ao tópico de retry: " + ex.getMessage(),
                    topico,
                    partition,
                    offset
                );

                publicarEventoPort.publicar(topicoDlqPedido, eventoRetry);

                log.error("[CONSUMER-PEDIDO] Reprocessamento esgotado e evento foi para DLQ | topicoOrigem={} topicoDlq={} offset={} tentativa={}/{} erro={}",
                    topico, topicoDlqPedido, offset, tentativaAtual, maxTentativasRetry, ex.getMessage());
                ack.acknowledge();
                return;
            }

            log.error("[CONSUMER-PEDIDO] Falha ao processar pedido | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
            throw new RuntimeException("Falha transitória no processamento de pedido", ex);
        }
    }

    private int obterTentativaAtual(Evento evento) {
        if (evento == null || evento.getTentativasRetry() == null || evento.getTentativasRetry() <= 0) {
            return 1;
        }
        return evento.getTentativasRetry();
    }
    
}
