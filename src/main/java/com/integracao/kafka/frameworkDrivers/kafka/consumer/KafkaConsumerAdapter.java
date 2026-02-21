package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.integracao.kafka.application.gateway.in.ProcessarEventoPort;
import com.integracao.kafka.domain.model.Evento;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerAdapter {

    private final ProcessarEventoPort processarEventoPort;

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

}
