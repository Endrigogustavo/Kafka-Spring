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

    @Value("${integrador.topico.entrada-nota:entrada.nota}")
    private String topicoEntradaNota;

    @Value("${integrador.topico.saida-nota:saida.nota}")
    private String topicoSaidaNota;

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
    public NewTopic topicoDlqEntrada() {
        // DLQ do tópico de entrada — mensagens que falharam N vezes
        return TopicBuilder.name(topicoEntrada + ".DLQ")
            .partitions(1)
            .replicas(1)
            .build();
    }

    // ─── Error Handler com DLQ ───────────────────────────────────────────────

    /**
     * Estratégia de erro:
     * 1. Tenta processar a mensagem com 3 tentativas, aguardando 1s entre cada
     * 2. Após 3 falhas, publica a mensagem no tópico .DLQ automaticamente
     * 3. O fluxo principal não é bloqueado
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // Recoverer: envia para o tópico .DLQ da mesma partição
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> {
                String dlqTopico = record.topic() + ".DLQ";
                log.error("[DLQ] Mensagem enviada para DLQ | topico={} dlq={} erro={}",
                    record.topic(), dlqTopico, ex.getMessage());
                return new TopicPartition(dlqTopico, -1); // -1 = qualquer partição
            });

        // BackOff: 3 tentativas com 1 segundo de intervalo
        var backOff = new FixedBackOff(1_000L, 3L);

        var handler = new DefaultErrorHandler(recoverer, backOff);

        // Erros de dados inválidos não devem ser retentados
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }
}
