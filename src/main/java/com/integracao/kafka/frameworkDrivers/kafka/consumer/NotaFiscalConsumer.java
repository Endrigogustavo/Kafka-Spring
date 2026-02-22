package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.service.NotaFiscalService;
import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberNotaUseCase;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;
import com.integracao.kafka.domain.model.NotaFiscal;

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

    @Value("${integrador.topico.saida-nota:integrador.nota.processado}")
    private String topicoSaidaNota;

    @Value("${integrador.topico.dlq-nota:integrador.nota.dlq}")
    private String topicoDlqNota;

    @Value("${integrador.topico.retry-nota:integrador.nota.retry}")
    private String topicoRetryNota;

    @Value("${integrador.reprocessamento.max-tentativas:5}")
    private int maxTentativasRetry;

    @KafkaListener(
        topics = "${integrador.topico.nota:integrador.nota.recebido}",
        groupId = "${spring.kafka.consumer.group-id:integrador-group}-notas"
    )
    public void consumirNotaEntrada(ConsumerRecord<String, Evento> record, Acknowledgment ack) {
        processar(record, ack, false);
    }

    @KafkaListener(
        topics = "${integrador.topico.retry-nota:integrador.nota.retry}",
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

            publicarEventoPort.publicar(topicoSaidaNota, eventoSaida);
            ack.acknowledge();

            log.info("[CONSUMER-NOTA] Nota fiscal publicada | topicoSaida={} numero={} eventoId={}",
                topicoSaidaNota, notaFiscal.getNumeroNota(), eventoSaida.getId());

        } catch (IllegalArgumentException ex) {
            gerenciarFalhasUseCase.registrarFalha(
                TipoFalha.NOTA,
                record.value(),
                ex.getMessage(),
                topico,
                partition,
                offset
            );

            publicarEventoPort.publicar(topicoDlqNota, record.value());

            log.warn("[CONSUMER-NOTA] Nota fiscal inválida enviada para DLQ | topicoOrigem={} topicoDlq={} offset={} erro={}",
                topico, topicoDlqNota, offset, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            if (origemErro) {
                Evento eventoRetry = record.value();
                int tentativaAtual = obterTentativaAtual(eventoRetry);

                if (tentativaAtual < maxTentativasRetry) {
                    eventoRetry.setTentativasRetry(tentativaAtual + 1);
                    eventoRetry.setStatus(Evento.StatusEvento.FALHA);
                    publicarEventoPort.publicar(topicoRetryNota, eventoRetry);

                    log.warn("[CONSUMER-NOTA] Reprocessamento falhou, reenviado para retry | topicoOrigem={} topicoRetry={} offset={} tentativa={}/{} erro={}",
                        topico, topicoRetryNota, offset, tentativaAtual + 1, maxTentativasRetry, ex.getMessage());
                    ack.acknowledge();
                    return;
                }

                gerenciarFalhasUseCase.registrarFalha(
                    TipoFalha.NOTA,
                    eventoRetry,
                    "Falha técnica após envio ao tópico de retry: " + ex.getMessage(),
                    topico,
                    partition,
                    offset
                );

                publicarEventoPort.publicar(topicoDlqNota, eventoRetry);

                log.error("[CONSUMER-NOTA] Reprocessamento esgotado e evento foi para DLQ | topicoOrigem={} topicoDlq={} offset={} tentativa={}/{} erro={}",
                    topico, topicoDlqNota, offset, tentativaAtual, maxTentativasRetry, ex.getMessage());
                ack.acknowledge();
                return;
            }

            log.error("[CONSUMER-NOTA] Falha ao processar nota fiscal | topico={} offset={} erro={}",
                topico, offset, ex.getMessage());
            throw new RuntimeException("Falha transitória no processamento de nota", ex);
        }
    }

    private int obterTentativaAtual(Evento evento) {
        if (evento == null || evento.getTentativasRetry() == null || evento.getTentativasRetry() <= 0) {
            return 1;
        }
        return evento.getTentativasRetry();
    }

}

