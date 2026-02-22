package com.integracao.kafka.frameworkDrivers.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
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

    @Value("${integrador.topico.entrada:integrador.evento.recebido}")
    private String topicoEntrada;

    @Value("${integrador.topico.saida:integrador.evento.processado}")
    private String topicoSaida;

    @Value("${integrador.topico.entrada-pedido:integrador.pedido.recebido}")
    private String topicoEntradaPedido;

    @Value("${integrador.topico.saida-pedido:integrador.pedido.processado}")
    private String topicoSaidaPedido;

    @Value("${integrador.topico.nota:integrador.nota.recebido}")
    private String topicoEntradaNota;

    @Value("${integrador.topico.saida-nota:integrador.nota.processado}")
    private String topicoSaidaNota;

    @Value("${integrador.topico.retry-evento:integrador.evento.retry}")
    private String topicoRetryEvento;

    @Value("${integrador.topico.retry-pedido:integrador.pedido.retry}")
    private String topicoRetryPedido;

    @Value("${integrador.topico.retry-nota:integrador.nota.retry}")
    private String topicoRetryNota;

    @Value("${integrador.topico.dlq-evento:integrador.evento.dlq}")
    private String topicoDlqEvento;

    @Value("${integrador.topico.dlq-pedido:integrador.pedido.dlq}")
    private String topicoDlqPedido;

    @Value("${integrador.topico.dlq-nota:integrador.nota.dlq}")
    private String topicoDlqNota;

    @Value("${integrador.topico.retencao-ms:604800000}")
    private long retencaoTopicosMs;

    @Value("${integrador.topico.retencao-dlq-ms:60480000000}")
    private long retencaoTopicosErroMs;

    // ─── Criação automática de tópicos ───────────────────────────────────────

    @Bean
    public NewTopic topicoEntrada() {
        return criarTopicoPadrao(topicoEntrada);
    }

    

    @Bean
    public NewTopic topicoEntradaNota() {
        return criarTopicoPadrao(topicoEntradaNota);
    }

    @Bean
    public NewTopic topicoSaidaNota() {
        return criarTopicoPadrao(topicoSaidaNota);
    }

    @Bean
    public NewTopic topicoSaida() {
        return criarTopicoPadrao(topicoSaida);
    }

    @Bean
    public NewTopic topicoEntradaPedido() {
        return criarTopicoPadrao(topicoEntradaPedido);
    }

    @Bean
    public NewTopic topicoSaidaPedido() {
        return criarTopicoPadrao(topicoSaidaPedido);
    }

    @Bean
    public NewTopic topicoRetryEvento() {
        return criarTopicoErro(topicoRetryEvento);
    }

    @Bean
    public NewTopic topicoRetryPedido() {
        return criarTopicoErro(topicoRetryPedido);
    }

    @Bean
    public NewTopic topicoRetryNota() {
        return criarTopicoErro(topicoRetryNota);
    }

    @Bean
    public NewTopic topicoDlqEvento() {
        return criarTopicoErro(topicoDlqEvento);
    }

    @Bean
    public NewTopic topicoDlqPedido() {
        return criarTopicoErro(topicoDlqPedido);
    }

    @Bean
    public NewTopic topicoDlqNota() {
        return criarTopicoErro(topicoDlqNota);
    }

    private NewTopic criarTopicoPadrao(String nomeTopico) {
        return TopicBuilder.name(nomeTopico)
            .partitions(3)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retencaoTopicosMs))
            .build();
    }

    private NewTopic criarTopicoErro(String nomeTopico) {
        return TopicBuilder.name(nomeTopico)
            .partitions(3)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retencaoTopicosErroMs))
            .build();
    }


    /**
     * Estratégia de erro:
     * 1. Tenta processar a mensagem com 3 tentativas, aguardando 1s entre cada
        * 2. Após tentativas esgotadas, publica no tópico integrador.<recurso>.retry
     * 3. O fluxo principal não é bloqueado
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // Recoverer: envia para o tópico de erro da mesma partição
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> {
                String topicoRetry = topicoRetryProcessamento(record.topic());
                log.error("[ERRO-PROCESSAMENTO] Mensagem enviada para tópico de retry | topico={} topicoRetry={} erro={}",
                    record.topic(), topicoRetry, ex.getMessage());
                return new TopicPartition(topicoRetry, record.partition());
            });

        // BackOff: 2 tentativas com 500ms para reduzir latência total de recuperação
        var backOff = new FixedBackOff(500L, 2L);

        var handler = new DefaultErrorHandler(recoverer, backOff);

        // Erros de dados inválidos não devem ser retentados
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }

    private String topicoRetryProcessamento(String topicoOrigem) {
        if (topicoEntradaPedido.equals(topicoOrigem)) {
            return topicoRetryPedido;
        }
        if (topicoEntradaNota.equals(topicoOrigem)) {
            return topicoRetryNota;
        }
        return topicoRetryEvento;
    }
}
