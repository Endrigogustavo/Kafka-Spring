package com.integracao.kafka.frameworkDrivers.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${integrador.topico.entrada:entrada.evento}")
    private String topicoEntrada;

    @Value("${integrador.topico.saida:saida.evento}")
    private String topicoSaida;

    @Value("${integrador.topico.entrada-pedido:entrada.pedido}")
    private String topicoEntradaPedido;

    @Value("${integrador.topico.saida-pedido:saida.pedido}")
    private String topicoSaidaPedido;

    @Value("${integrador.topico.nota:entrada.nota}")
    private String topicoEntradaNota;

    @Value("${integrador.topico.saida-nota:saida.nota}")
    private String topicoSaidaNota;

    @Value("${integrador.topico.erro-evento:erro.evento}")
    private String topicoErroEvento;

    @Value("${integrador.topico.erro-pedido:erro.pedido}")
    private String topicoErroPedido;

    @Value("${integrador.topico.erro-nota:erro.nota}")
    private String topicoErroNota;

    // ─── Criação automática de tópicos ───────────────────────────────────────

    @Bean
    public NewTopic topicoEntrada() {
        return TopicBuilder.name(topicoEntrada)
            .partitions(3)   // paralelismo
            .replicas(1)     // aumente para 3 em produção com cluster
            .build();
    }

    

    @Bean
    public NewTopic topicoEntradaNota() {
        return TopicBuilder.name(topicoEntradaNota)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoSaidaNota() {
        return TopicBuilder.name(topicoSaidaNota)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoSaida() {
        return TopicBuilder.name(topicoSaida)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoEntradaPedido() {
        return TopicBuilder.name(topicoEntradaPedido)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoSaidaPedido() {
        return TopicBuilder.name(topicoSaidaPedido)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoErroEntrada() {
        return TopicBuilder.name(topicoErroEvento)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoErroPedido() {
        return TopicBuilder.name(topicoErroPedido)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic topicoErroNota() {
        return TopicBuilder.name(topicoErroNota)
            .partitions(3)
            .replicas(1)
            .build();
    }

    // ─── Error Handler com tópico de erro de processamento ───────────────────

    /**
     * Estratégia de erro:
     * 1. Tenta processar a mensagem com 3 tentativas, aguardando 1s entre cada
        * 2. Após tentativas esgotadas, publica no tópico erro.<dominio>
     * 3. O fluxo principal não é bloqueado
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // Recoverer: envia para o tópico de erro da mesma partição
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> {
                String topicoErro = topicoErroProcessamento(record.topic());
                log.error("[ERRO-PROCESSAMENTO] Mensagem enviada para tópico de erro | topico={} topicoErro={} erro={}",
                    record.topic(), topicoErro, ex.getMessage());
                return new TopicPartition(topicoErro, record.partition());
            });

        // BackOff: 2 tentativas com 500ms para reduzir latência total de recuperação
        var backOff = new FixedBackOff(500L, 2L);

        var handler = new DefaultErrorHandler(recoverer, backOff);

        // Erros de dados inválidos não devem ser retentados
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }

    private String topicoErroProcessamento(String topicoOrigem) {
        if (topicoEntradaPedido.equals(topicoOrigem)) {
            return topicoErroPedido;
        }
        if (topicoEntradaNota.equals(topicoOrigem)) {
            return topicoErroNota;
        }
        return topicoErroEvento;
    }
}
