package com.integracao.kafka.application.metrics;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

/**
 * Métricas customizadas expostas ao Prometheus.
 * Visualizadas no Grafana em tempo real.
 *
 * Métricas disponíveis:
 *   integrador_mensagens_total{status="sucesso|falha"}  — contador de mensagens
 *   integrador_processamento_segundos                   — histograma de latência
 */
@Slf4j
@Component
public class IntegradorMetrics {

    private final Counter mensagensSucesso;
    private final Counter mensagensFalha;
    private final Timer   tempoProcessamento;

    public IntegradorMetrics(MeterRegistry registry) {
        this.mensagensSucesso = Counter.builder("integrador_mensagens_total")
            .description("Total de mensagens processadas pelo integrador")
            .tag("status", "sucesso")
            .register(registry);

        this.mensagensFalha = Counter.builder("integrador_mensagens_total")
            .description("Total de mensagens processadas pelo integrador")
            .tag("status", "falha")
            .register(registry);

        this.tempoProcessamento = Timer.builder("integrador_processamento_segundos")
            .description("Tempo de processamento de cada mensagem")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
    }

    public void registrarSucesso() {
        mensagensSucesso.increment();
    }

    public void registrarFalha() {
        mensagensFalha.increment();
    }

    /**
     * Executa a operação medindo o tempo automaticamente.
     * Uso: metrics.registrarTempo(() -> { ... logica ... });
     */
    public void registrarTempo(Runnable operacao) {
        tempoProcessamento.record(operacao);
    }
}
