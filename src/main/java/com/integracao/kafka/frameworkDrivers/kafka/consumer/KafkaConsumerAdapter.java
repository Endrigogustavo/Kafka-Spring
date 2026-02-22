package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.integracao.kafka.application.gateway.in.ProcessarEventoPort;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.domain.model.Evento;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerAdapter {

    private final ProcessarEventoPort processarEventoPort;
    private final PublicarEventoPort publicarEventoPort;

    @Value("${integrador.topico.dlq-evento:integrador.evento.dlq}")
    private String topicoDlqEvento;

    @KafkaListener(
        topics  = "${integrador.topico.entrada:integrador.evento.recebido}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}"
    )
    public void consumir(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, false);
    }

    @KafkaListener(
        topics  = "${integrador.topico.retry-evento:integrador.evento.retry}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-reprocessamento"
    )
    public void reprocessar(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, true);
    }

    private void processar(ConsumerRecord<String, Evento> record, Acknowledgment ack, boolean origemRetry) {
        String topico  = record.topic();
        long   offset  = record.offset();
        int    particao = record.partition();

        log.info("[CONSUMER] Mensagem recebida | topico={} particao={} offset={} origemRetry={}",
            topico, particao, offset, origemRetry);

        try {
            Evento evento = record.value();
            processarEventoPort.executar(evento);

            // Confirma offset apenas após sucesso — mensagem não será reentregue
            ack.acknowledge();
            log.info("[CONSUMER] ACK confirmado | topico={} offset={}", topico, offset);

        } catch (IllegalArgumentException ex) {
            publicarEventoPort.publicar(topicoDlqEvento, record.value());
            log.warn("[CONSUMER] Mensagem inválida enviada para DLQ | topicoOrigem={} topicoDlq={} offset={} erro={}",
                topico, topicoDlqEvento, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            if (origemRetry) {
                publicarEventoPort.publicar(topicoDlqEvento, record.value());
                log.error("[CONSUMER] Reprocessamento falhou e evento foi para DLQ | topicoOrigem={} topicoDlq={} offset={} erro={}",
                    topico, topicoDlqEvento, offset, ex.getMessage());
                ack.acknowledge();
                return;
            }

            // Erro de infraestrutura: NÃO faz ACK → Kafka reentrega após intervalo
            // Após N tentativas configuradas no DefaultErrorHandler, vai para o tópico retry
            log.error("[CONSUMER] Falha ao processar | offset={} erro={} — sem ACK, será reenfileirado", offset, ex.getMessage());
            // Não chama ack.acknowledge() → mensagem volta para a fila
        }
    }

}
