package com.integracao.kafka.application.useCase.publish;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.integracao.kafka.application.gateway.in.PublicarNotaPort;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.metrics.IntegradorMetrics;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.NotaFiscal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PublicarNotaFiscalUseCase implements PublicarNotaPort {
  private final PublicarEventoPort publicarEventoPort;
    private final IntegradorMetrics metrics;
    
    private static final String TOPICO_ENTRADA_NOTA_FISCAL = "integrador.nota.recebido";

    @Override
    public String executar(NotaFiscal notaFiscal) {
        log.info("[ORQUESTRADOR-NOTA-FISCAL] Iniciando criação de nota fiscal | cliente={} produto={}", 
            notaFiscal.getCliente(), notaFiscal.getProduto());

        String eventoId;
        try {
            // Passo 1: Valida
            validar(notaFiscal);

            // Passo 2: Gera número da nota fiscal se não fornecido
            if (notaFiscal.getNumeroNota() == null || notaFiscal.getNumeroNota().isEmpty()) {
                notaFiscal.setNumeroNota("NF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
            }
       
            // Passo 3: Encapsula em Evento
            Evento evento = Evento.builder()
                    .tipo("NOTA_FISCAL_CRIADA")
                    .origem("API_REST")
                    .destino("SISTEMA_NOTA_FISCAL")
                    .payload(notaFiscal)
                    .status(Evento.StatusEvento.RECEBIDO)
                    .build();

            // Passo 5: Publica via port de saída (com registro de tempo)
            metrics.registrarTempo(() -> {
                publicarEventoPort.publicar(TOPICO_ENTRADA_NOTA_FISCAL, evento);
            });
            
            eventoId = evento.getId();
            metrics.registrarSucesso();
            
            log.info("[ORQUESTRADOR-NOTA-FISCAL] Nota fiscal criada com sucesso | numero={} eventoId={}", 
                notaFiscal.getNumeroNota(), eventoId);
            
            return eventoId;

        } catch (IllegalArgumentException ex) {
            metrics.registrarFalha();
            log.error("[ORQUESTRADOR-NOTA-FISCAL] Dados inválidos | erro={}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            metrics.registrarFalha();
            log.error("[ORQUESTRADOR-NOTA-FISCAL] Falha ao criar nota fiscal | erro={}", ex.getMessage());
            throw new RuntimeException("Falha ao criar nota fiscal: " + ex.getMessage(), ex);
        }
    }

    private void validar(NotaFiscal notaFiscal) {
        if (notaFiscal == null) {
            throw new IllegalArgumentException("Nota fiscal não pode ser nula");
        }
        if (notaFiscal.getValorTotal() == null) {
            throw new IllegalArgumentException("Valor total é obrigatório");
        }
        log.debug("[ORQUESTRADOR-NOTA-FISCAL] Nota fiscal validada com sucesso | cliente={} produto={}", 
            notaFiscal.getCliente(), notaFiscal.getProduto());
    }
}
