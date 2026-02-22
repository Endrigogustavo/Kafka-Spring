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

    @Value("${integrador.topico.retencao-ms:604800000}")
    private long retencaoTopicosMs;

    @Value("${integrador.topico.retencao-ms:60480000000}")
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
    public NewTopic topicoErroEntrada() {
        return criarTopicoErro(topicoErroEvento);
    }

    @Bean
    public NewTopic topicoErroPedido() {
        return criarTopicoErro(topicoErroPedido);
    }

    @Bean
    public NewTopic topicoErroNota() {
        return criarTopicoErro(topicoErroNota);
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
