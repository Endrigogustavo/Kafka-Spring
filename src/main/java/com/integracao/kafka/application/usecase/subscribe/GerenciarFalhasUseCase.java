package com.integracao.kafka.application.useCase.subscribe;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.FalhaProcessamento;
import com.integracao.kafka.domain.model.FalhaProcessamento.StatusFalha;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class GerenciarFalhasUseCase {

    private final PublicarEventoPort publicarEventoPort;
    private final String topicoEntradaPedido;
    private final String topicoEntradaNota;
    private final int limiteHistorico;
    private final int maxTentativasReprocessamento;
    private final int intervaloTentativaSegundos;

    private final Map<String, FalhaProcessamento> falhas = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<String> ordemFalhas = new ConcurrentLinkedDeque<>();
    private final Map<String, String> indiceFalhaPorTipoEvento = new ConcurrentHashMap<>();

    public GerenciarFalhasUseCase(
        PublicarEventoPort publicarEventoPort,
        @Value("${integrador.topico.entrada-pedido:integrador.pedido.recebido}") String topicoEntradaPedido,
        @Value("${integrador.topico.nota:integrador.nota.recebido}") String topicoEntradaNota,
        @Value("${integrador.historico.falhas.limite:1000}") int limiteHistorico,
        @Value("${integrador.reprocessamento.max-tentativas:5}") int maxTentativasReprocessamento,
        @Value("${integrador.reprocessamento.intervalo-segundos:60}") int intervaloTentativaSegundos
    ) {
        this.publicarEventoPort = publicarEventoPort;
        this.topicoEntradaPedido = topicoEntradaPedido;
        this.topicoEntradaNota = topicoEntradaNota;
        this.limiteHistorico = Math.max(1, limiteHistorico);
        this.maxTentativasReprocessamento = Math.max(1, maxTentativasReprocessamento);
        this.intervaloTentativaSegundos = Math.max(1, intervaloTentativaSegundos);
    }

    public FalhaProcessamento registrarFalha(
        TipoFalha tipo,
        Evento eventoOriginal,
        String motivo,
        String topicoOrigem,
        int particaoOrigem,
        long offsetOrigem
    ) {
        String chaveEvento = gerarChaveEvento(tipo, eventoOriginal);
        FalhaProcessamento falhaExistente = buscarFalhaExistente(chaveEvento);

        if (falhaExistente != null) {
            falhaExistente.setEventoOriginal(eventoOriginal);
            falhaExistente.setMotivo(motivo);
            falhaExistente.setTopicoOrigem(topicoOrigem);
            falhaExistente.setParticaoOrigem(particaoOrigem);
            falhaExistente.setOffsetOrigem(offsetOrigem);

            if (atingiuLimiteTentativas(falhaExistente)) {
                falhaExistente.setStatus(StatusFalha.ESGOTADO);
            } else {
                falhaExistente.setStatus(StatusFalha.PENDENTE_REPROCESSAMENTO);
            }

            log.warn("[FALHAS] Falha atualizada | id={} tipo={} tentativas={}/{} status={} motivo={}",
                falhaExistente.getId(),
                falhaExistente.getTipo(),
                falhaExistente.getTentativasReprocessamento(),
                falhaExistente.getMaxTentativasReprocessamento(),
                falhaExistente.getStatus(),
                motivo);

            return falhaExistente;
        }

        FalhaProcessamento falha = FalhaProcessamento.builder()
            .tipo(tipo)
            .eventoOriginal(eventoOriginal)
            .motivo(motivo)
            .topicoOrigem(topicoOrigem)
            .particaoOrigem(particaoOrigem)
            .offsetOrigem(offsetOrigem)
            .status(StatusFalha.PENDENTE_REPROCESSAMENTO)
            .tentativasReprocessamento(0)
            .maxTentativasReprocessamento(maxTentativasReprocessamento)
            .build();

        falhas.put(falha.getId(), falha);
        ordemFalhas.addLast(falha.getId());
        if (chaveEvento != null) {
            indiceFalhaPorTipoEvento.put(chaveEvento, falha.getId());
        }
        aplicarLimiteHistorico();

        log.warn("[FALHAS] Falha registrada | id={} tipo={} topico={} particao={} offset={} motivo={}",
            falha.getId(), tipo, topicoOrigem, particaoOrigem, offsetOrigem, motivo);

        return falha;
    }

    public List<FalhaProcessamento> listarFalhas(TipoFalha tipo, StatusFalha status, int limite) {
        List<FalhaProcessamento> ordenadas = new ArrayList<>();
        for (String id : ordemFalhas) {
            FalhaProcessamento falha = falhas.get(id);
            if (falha != null) {
                ordenadas.add(falha);
            }
        }

        List<FalhaProcessamento> filtradas = ordenadas.stream()
            .filter(f -> tipo == null || f.getTipo() == tipo)
            .filter(f -> status == null || f.getStatus() == status)
            .toList();

        if (filtradas.isEmpty()) {
            return List.of();
        }

        int limiteAjustado = limite <= 0 ? filtradas.size() : Math.min(limite, filtradas.size());
        int inicio = Math.max(0, filtradas.size() - limiteAjustado);
        return List.copyOf(filtradas.subList(inicio, filtradas.size()));
    }

    public FalhaProcessamento reprocessar(String idFalha) {
        FalhaProcessamento falha = falhas.get(idFalha);
        if (falha == null) {
            throw new IllegalArgumentException("Falha não encontrada para o id=" + idFalha);
        }

        if (falha.getStatus() == StatusFalha.ESGOTADO) {
            throw new IllegalArgumentException("Falha esgotada. Não é mais elegível para reprocessamento automático");
        }

        if (falha.getStatus() != StatusFalha.PENDENTE_REPROCESSAMENTO) {
            throw new IllegalArgumentException("Falha não está pendente para reprocessamento. status=" + falha.getStatus());
        }

        if (atingiuLimiteTentativas(falha)) {
            falha.setStatus(StatusFalha.ESGOTADO);
            throw new IllegalArgumentException("Falha atingiu limite de tentativas de reprocessamento");
        }

        LocalDateTime agora = LocalDateTime.now();
        if (falha.getProximaTentativaPermitidaEm() != null && agora.isBefore(falha.getProximaTentativaPermitidaEm())) {
            throw new IllegalArgumentException("Reprocessamento bloqueado até " + falha.getProximaTentativaPermitidaEm());
        }

        if (falha.getEventoOriginal() == null || Objects.isNull(falha.getEventoOriginal().getPayload())) {
            throw new IllegalArgumentException("Falha sem evento/payload para reprocessar");
        }

        String topicoEntrada = falha.getTipo() == TipoFalha.PEDIDO ? topicoEntradaPedido : topicoEntradaNota;

        falha.setTentativasReprocessamento(falha.getTentativasReprocessamento() + 1);
        falha.setUltimaTentativaReprocessamentoEm(agora);
        falha.setProximaTentativaPermitidaEm(agora.plusSeconds(intervaloTentativaSegundos));

        if (atingiuLimiteTentativas(falha)) {
            falha.setStatus(StatusFalha.ESGOTADO);
        }

        publicarEventoPort.publicar(topicoEntrada, falha.getEventoOriginal());

        if (falha.getStatus() != StatusFalha.ESGOTADO) {
            falha.setStatus(StatusFalha.REPROCESSADO);
        }
        falha.setReprocessadoEm(LocalDateTime.now());

        log.info("[FALHAS] Falha reprocessada | id={} tipo={} topicoEntrada={} tentativas={}/{} status={}",
            falha.getId(),
            falha.getTipo(),
            topicoEntrada,
            falha.getTentativasReprocessamento(),
            falha.getMaxTentativasReprocessamento(),
            falha.getStatus());

        return falha;
    }

    public FalhaProcessamento descartar(String idFalha) {
        FalhaProcessamento falha = falhas.get(idFalha);
        if (falha == null) {
            throw new IllegalArgumentException("Falha não encontrada para o id=" + idFalha);
        }

        falha.setStatus(StatusFalha.DESCARTADO);
        falha.setReprocessadoEm(LocalDateTime.now());
        return falha;
    }

    private void aplicarLimiteHistorico() {
        while (ordemFalhas.size() > limiteHistorico) {
            String removido = ordemFalhas.pollFirst();
            if (removido != null) {
                FalhaProcessamento falhaRemovida = falhas.remove(removido);
                if (falhaRemovida != null) {
                    String chave = gerarChaveEvento(falhaRemovida.getTipo(), falhaRemovida.getEventoOriginal());
                    if (chave != null) {
                        indiceFalhaPorTipoEvento.remove(chave);
                    }
                }
            }
        }
    }

    private FalhaProcessamento buscarFalhaExistente(String chaveEvento) {
        if (chaveEvento == null) {
            return null;
        }

        String idFalha = indiceFalhaPorTipoEvento.get(chaveEvento);
        return idFalha == null ? null : falhas.get(idFalha);
    }

    private String gerarChaveEvento(TipoFalha tipo, Evento evento) {
        if (tipo == null || evento == null || evento.getId() == null || evento.getId().isBlank()) {
            return null;
        }
        return tipo.name() + "::" + evento.getId();
    }

    private boolean atingiuLimiteTentativas(FalhaProcessamento falha) {
        return falha.getTentativasReprocessamento() >= falha.getMaxTentativasReprocessamento();
    }
}
