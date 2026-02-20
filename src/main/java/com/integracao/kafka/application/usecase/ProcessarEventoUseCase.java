package com.integracao.kafka.application.usecase;

import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.Evento.StatusEvento;
import com.integracao.kafka.infrastructure.metrics.IntegradorMetrics;
import com.integracao.kafka.port.in.ProcessarEventoPort;
import com.integracao.kafka.port.out.PublicarEventoPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Use Case / Orquestrador central.
 *
 * Responsabilidade: controlar o fluxo completo de integração.
 * Não conhece Kafka, HTTP ou qualquer tecnologia — só as portas.
 *
 * Fluxo:
 *   1. Recebe o evento via Port de Entrada
 *   2. Valida
 *   3. Transforma (adapta ao contrato do Sistema B)
 *   4. Publica via Port de Saída
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessarEventoUseCase implements ProcessarEventoPort {

    private final PublicarEventoPort publicarEventoPort;
    private final IntegradorMetrics  metrics;

    @Value("${integrador.topico.saida:saida.evento}")
    private String topicoSaida;

    @Override
    public void executar(Evento evento) {
        log.info("[ORQUESTRADOR] Iniciando processamento | id={} tipo={}", evento.getId(), evento.getTipo());

        metrics.registrarTempo(() -> {
            try {
                // Passo 1: Valida
                validar(evento);

                // Passo 2: Atualiza status
                evento.setStatus(StatusEvento.PROCESSANDO);

                // Passo 3: Transforma (adicione regras de negócio aqui)
                Evento eventoTransformado = transformar(evento);

                // Passo 4: Publica via port de saída
                eventoTransformado.setStatus(StatusEvento.ENVIADO);
                publicarEventoPort.publicar(topicoSaida, eventoTransformado);

                metrics.registrarSucesso();
                log.info("[ORQUESTRADOR] Evento processado com sucesso | id={}", evento.getId());

            } catch (Exception ex) {
                evento.setStatus(StatusEvento.FALHA);
                metrics.registrarFalha();
                log.error("[ORQUESTRADOR] Falha ao processar evento | id={} erro={}", evento.getId(), ex.getMessage());
                throw ex; // propaga para o adaptador lidar (DLQ, retry etc.)
            }
        });
    }

    private void validar(Evento evento) {
        if (evento.getPayload() == null) {
            throw new IllegalArgumentException("Payload do evento nao pode ser nulo | id=" + evento.getId());
        }
        if (evento.getTipo() == null || evento.getTipo().isBlank()) {
            throw new IllegalArgumentException("Tipo do evento nao pode ser vazio | id=" + evento.getId());
        }
        log.debug("[ORQUESTRADOR] Evento validado | id={}", evento.getId());
    }

    private Evento transformar(Evento evento) {
        // Ponto de extensão: adicione mapeamentos, enriquecimento de dados, etc.
        // Exemplo: buscar dados complementares de uma API, converter formatos, etc.
        log.debug("[ORQUESTRADOR] Evento transformado | id={}", evento.getId());
        return evento;
    }
}
