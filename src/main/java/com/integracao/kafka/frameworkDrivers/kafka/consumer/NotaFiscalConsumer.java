package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.useCase.subscribe.ReceberNotaUseCase;
import com.integracao.kafka.domain.entity.Evento;
import com.integracao.kafka.domain.entity.NotaFiscal; 

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotaFiscalConsumer {
    private final PublicarEventoPort publicarEventoPort;
    private final ObjectMapper objectMapper;
    private final ReceberNotaUseCase receberNotaUseCase;

    @KafkaListener(topics = "${integrador.topico.nota:entrada.nota}", groupId = "${spring.kafka.consumer.group-id:integrador-group}-notas")
    public void consumirNotaEntrada(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        String topico = record.topic();
        long offset = record.offset();
        int partition = record.partition();

        log.info("[CONSUMER-NOTA] Mensagem recebida | topico={} particao={} offset={}", topico, partition, offset);

        try {
            Evento eventoEntrada = record.value();
            NotaFiscal notaFiscal = objectMapper.convertValue(eventoEntrada.getPayload(), NotaFiscal.class);

            if (notaFiscal.getNumeroNota() == null || notaFiscal.getNumeroNota().isBlank()) {
                throw new IllegalArgumentException("Nota fiscal sem numeroNota no payload");
            }

            notaFiscal.setDataProcessamento(java.time.LocalDateTime.now());
            notaFiscal.setStatusProcessamento("PROCESSADO");
            notaFiscal.setKafkaOffset(offset);
            notaFiscal.setKafkaPartition(partition);
            notaFiscal.setKafkaTopic(topico);
            notaFiscal.setKafkaTimestamp(java.time.Instant.ofEpochMilli(record.timestamp()));

            log.info("[CONSUMER-NOTA] Nota fiscal enriquecida | numero={} offset={} partition={}",
                    notaFiscal.getNumeroNota(), offset, partition);

            // Cria evento de saída e publica
            Evento eventoSaida = Evento.builder()
                    .tipo("NOTA_FISCAL_PROCESSADA")
                    .origem("CONSUMER_KAFKA")
                    .destino("SISTEMA_NOTAS")
                    .payload(notaFiscal)
                    .status(Evento.StatusEvento.ENVIADO)
                    .build();

            publicarEventoPort.publicar("saida.nota", eventoSaida);
            ack.acknowledge();

            log.info("[CONSUMER-NOTA] Nota fiscal publicada em saida.nota | numero={} eventoId={}",
                    notaFiscal.getNumeroNota(), eventoSaida.getId());

        } catch (IllegalArgumentException ex) {
            log.warn("[CONSUMER-NOTA] Nota fiscal inválida descartada | topico={} offset={} erro={}",
                    topico, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[CONSUMER-NOTA] Falha ao processar nota fiscal | topico={} offset={} erro={}",
                    topico, offset, ex.getMessage());
        }
    }
}
