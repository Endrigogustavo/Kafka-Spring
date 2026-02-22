package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.service.NotaFiscalService;
import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberNotaUseCase;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.NotaFiscal;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotaFiscalConsumer {
    private final PublicarEventoPort publicarEventoPort;
    private final ObjectMapper objectMapper;
    private final ReceberNotaUseCase receberNotaUseCase;
    private final GerenciarFalhasUseCase gerenciarFalhasUseCase;
    private final NotaFiscalService notaFiscalService;

    @KafkaListener(
        topics = "${integrador.topico.nota:entrada.nota}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-notas"
    )
    public void consumirNotaEntrada(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, false);
    }

    @KafkaListener(
        topics = "${integrador.topico.erro-nota:erro.nota}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-notas-reprocessamento"
    )
    public void reprocessarNotaComErro(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, true);
    }

    private void processar(ConsumerRecord<String, Evento> record, Acknowledgment ack, boolean origemErro) {
        String topico = record.topic();
        long offset = record.offset();
        int partition = record.partition();

        log.info("[CONSUMER-NOTA] Mensagem recebida | topico={} particao={} offset={} origemErro={}",
            topico, partition, offset, origemErro);

        try {
            Evento eventoEntrada = record.value();
            NotaFiscal notaFiscal = objectMapper.convertValue(eventoEntrada.getPayload(), NotaFiscal.class);

            if (notaFiscal.getNumeroNota() == null || notaFiscal.getNumeroNota().isBlank()) {
                throw new IllegalArgumentException("Nota fiscal sem numeroNota no payload");
            }

            log.info("[CONSUMER-NOTA] Enviando nota fiscal para persistencia no banco | numero={} cliente={} produto={} topico={} particao={} offset={}",
                notaFiscal.getNumeroNota(), notaFiscal.getCliente(), notaFiscal.getProduto(), topico, partition, offset);
            notaFiscalService.criarNotaFiscalEntity(notaFiscal);
            log.info("[CONSUMER-NOTA] Persistencia de nota fiscal concluida no banco | numero={} topico={} particao={} offset={}",
                notaFiscal.getNumeroNota(), topico, partition, offset);

            notaFiscal.setDataProcessamento(java.time.LocalDateTime.now());
            notaFiscal.setStatusProcessamento("PROCESSADO");
            notaFiscal.setKafkaOffset(offset);
            notaFiscal.setKafkaPartition(partition);
            notaFiscal.setKafkaTopic(topico);
            notaFiscal.setKafkaTimestamp(java.time.Instant.ofEpochMilli(record.timestamp()));

            receberNotaUseCase.registrar(notaFiscal);

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
            gerenciarFalhasUseCase.registrarFalha(
                TipoFalha.NOTA,
                record.value(),
                ex.getMessage(),
                topico,
                partition,
                offset
            );

            log.warn("[CONSUMER-NOTA] Nota fiscal inválida descartada | topico={} offset={} erro={}",
                    topico, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            if (origemErro) {
                gerenciarFalhasUseCase.registrarFalha(
                    TipoFalha.NOTA,
                    record.value(),
                    "Falha técnica após envio ao tópico de erro: " + ex.getMessage(),
                    topico,
                    partition,
                    offset
                );

                log.error("[CONSUMER-NOTA] Reprocessamento falhou e foi armazenado para controle manual | topico={} offset={} erro={}",
                    topico, offset, ex.getMessage());
                ack.acknowledge();
                return;
            }

            log.error("[CONSUMER-NOTA] Falha ao processar nota fiscal | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
            throw new RuntimeException("Falha transitória no processamento de nota", ex);
        }
    }

}

