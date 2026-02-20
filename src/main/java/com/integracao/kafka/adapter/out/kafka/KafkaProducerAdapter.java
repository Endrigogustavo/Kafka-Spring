package com.integracao.kafka.adapter.out.kafka;

import java.util.concurrent.TimeUnit;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.port.out.PublicarEventoPort;

import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducerAdapter implements PublicarEventoPort {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Bulkhead(name = "kafka-producer")
    @CircuitBreaker(name = "kafka-producer", fallbackMethod = "fallback")
    @Retry(name = "kafka-producer")
    public void publicar(String topico, Evento evento) {
        log.info("[PRODUCER] Publicando evento | topico={} id={}", topico, evento.getId());

        try {
            var result = kafkaTemplate.send(topico, evento.getId(), evento).get(10, TimeUnit.SECONDS);
            log.info("[PRODUCER] Publicado com sucesso | topico={} id={} offset={}",
                topico, evento.getId(),
                result.getRecordMetadata().offset());
        } catch (Exception ex) {
            log.error("[PRODUCER] Falha ao publicar | topico={} id={} erro={}", topico, evento.getId(), ex.getMessage());
            throw new RuntimeException("Falha ao publicar no Kafka", ex);
        }
    }

    @SuppressWarnings("unused")
    public void fallback(String topico, Evento evento, Exception ex) {
        log.error("[PRODUCER] FALLBACK ativado | topico={} id={} motivo={}", topico, evento.getId(), ex.getMessage());
        // TODO: implementar Outbox Pattern aqui para garantir at-least-once
        // outboxRepository.salvar(topico, evento);
        throw new RuntimeException("Circuito aberto ou retries esgotados. Evento id=" + evento.getId() + " não publicado.", ex);
    }
}
