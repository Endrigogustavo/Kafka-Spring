package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.repository.NotaFiscalRepository;
import com.integracao.kafka.application.service.NotaFiscalService;
import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberNotaUseCase;
import com.integracao.kafka.domain.entity.NotaFiscalEntity;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;
import com.integracao.kafka.domain.model.NotaFiscal;

class NotaFiscalConsumerTest {

    private TestPublicarEventoPort publicarEventoPort;
    private ReceberNotaUseCase receberNotaUseCase;
    private GerenciarFalhasUseCase gerenciarFalhasUseCase;
    private NotaFiscalService notaFiscalService;
    private TestAcknowledgment acknowledgment;

    private NotaFiscalConsumer consumer;

    @BeforeEach
    void setUp() {
        publicarEventoPort = new TestPublicarEventoPort();
        receberNotaUseCase = new ReceberNotaUseCase(100);
        gerenciarFalhasUseCase = new GerenciarFalhasUseCase(
            publicarEventoPort,
            "integrador.pedido.recebido",
            "integrador.nota.recebido",
            100,
            3,
            1
        );
        notaFiscalService = new NotaFiscalServiceSempreFalha();
        acknowledgment = new TestAcknowledgment();

        consumer = new NotaFiscalConsumer(
            publicarEventoPort,
            new ObjectMapper().findAndRegisterModules(),
            receberNotaUseCase,
            gerenciarFalhasUseCase,
            notaFiscalService
        );

        ReflectionTestUtils.setField(consumer, "topicoSaidaNota", "integrador.nota.processado");
        ReflectionTestUtils.setField(consumer, "topicoDlqNota", "integrador.nota.dlq");
        ReflectionTestUtils.setField(consumer, "topicoRetryNota", "integrador.nota.retry");
        ReflectionTestUtils.setField(consumer, "maxTentativasRetry", 3);
    }

    @Test
    void deveReenviarParaRetryQuandoFalharNoReprocessamentoEAindaNaoEsgotouTentativas() {
        Evento evento = criarEventoNotaValido();
        evento.setTentativasRetry(1);

        ConsumerRecord<String, Evento> record = new ConsumerRecord<>(
            "integrador.nota.retry", 1, 21L, "key-1", evento
        );

        consumer.reprocessarNotaComErro(record, acknowledgment);

        assertEquals(1, publicarEventoPort.publicacoes.size());
        assertEquals("integrador.nota.retry", publicarEventoPort.publicacoes.get(0).topico());
        Evento reenviado = publicarEventoPort.publicacoes.get(0).evento();
        assertEquals(2, reenviado.getTentativasRetry());
        assertEquals(Evento.StatusEvento.FALHA, reenviado.getStatus());
        assertTrue(acknowledgment.acknowledged);
        assertTrue(gerenciarFalhasUseCase.listarFalhas(TipoFalha.NOTA, null, 100).isEmpty());
    }

    @Test
    void deveEnviarParaDlqQuandoFalharNoReprocessamentoEAtingirLimite() {
        Evento evento = criarEventoNotaValido();
        evento.setTentativasRetry(3);

        ConsumerRecord<String, Evento> record = new ConsumerRecord<>(
            "integrador.nota.retry", 1, 22L, "key-2", evento
        );

        consumer.reprocessarNotaComErro(record, acknowledgment);

        assertEquals(1, publicarEventoPort.publicacoes.size());
        assertEquals("integrador.nota.dlq", publicarEventoPort.publicacoes.get(0).topico());
        assertEquals(evento, publicarEventoPort.publicacoes.get(0).evento());
        assertTrue(acknowledgment.acknowledged);
        assertEquals(1, gerenciarFalhasUseCase.listarFalhas(TipoFalha.NOTA, null, 100).size());
    }

    private Evento criarEventoNotaValido() {
        NotaFiscal nota = NotaFiscal.builder()
            .numeroNota("NF-100")
            .cliente("Cliente Teste")
            .produto("Produto Y")
            .quantidade(2)
            .valorTotal(new BigDecimal("150.00"))
            .build();

        return Evento.builder()
            .tipo("NOTA_FISCAL_CRIADA")
            .origem("TESTE")
            .destino("SISTEMA_NOTAS")
            .payload(nota)
            .status(Evento.StatusEvento.RECEBIDO)
            .tentativasRetry(1)
            .build();
    }

    private record Publicacao(String topico, Evento evento) {
    }

    private static class TestPublicarEventoPort implements PublicarEventoPort {
        private final List<Publicacao> publicacoes = new ArrayList<>();

        @Override
        public void publicar(String topico, Evento evento) {
            publicacoes.add(new Publicacao(topico, evento));
        }
    }

    private static class TestAcknowledgment implements Acknowledgment {
        private boolean acknowledged;

        @Override
        public void acknowledge() {
            this.acknowledged = true;
        }
    }

    private static class NotaFiscalServiceSempreFalha extends NotaFiscalService {
        NotaFiscalServiceSempreFalha() {
            super(new NotaFiscalRepository() {
                @Override
                public NotaFiscalEntity save(NotaFiscalEntity notaFiscalEntity) {
                    return notaFiscalEntity;
                }

                @Override
                public NotaFiscalEntity findById(Long id) {
                    return null;
                }

                @Override
                public List<NotaFiscalEntity> findAll() {
                    return List.of();
                }
            });
        }

        @Override
        public NotaFiscalEntity criarNotaFiscalEntity(NotaFiscal notaFiscal) {
            throw new RuntimeException("falha persistencia");
        }
    }
}
