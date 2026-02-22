package com.integracao.kafka.application.service;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import com.integracao.kafka.application.repository.NotaFiscalRepository;
import com.integracao.kafka.domain.entity.NotaFiscalEntity;
import com.integracao.kafka.domain.entity.PedidoEntity;
import com.integracao.kafka.domain.model.NotaFiscal;
import com.integracao.kafka.domain.model.Pedido;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotaFiscalService {
    private final NotaFiscalRepository notaFiscalRepository;

    @Value("${integrador.persistencia.retry-interval-ms:5000}")
    private long retryIntervalMs;

    @Value("${integrador.persistencia.retry-max-interval-ms:30000}")
    private long retryMaxIntervalMs;

     public NotaFiscalEntity criarNotaFiscalEntity(NotaFiscal notaFiscal) {
         log.info("[SERVICE-NOTA-FISCAL] Iniciando persistencia de nota fiscal | numeroNota={} cliente={} produto={} quantidade={} valorTotal={}",
            notaFiscal.getNumeroNota(), notaFiscal.getCliente(), notaFiscal.getProduto(), notaFiscal.getQuantidade(), notaFiscal.getValorTotal());
        log.info("[SERVICE-NOTA-FISCAL] Iniciando persistencia de nota fiscal | numeroNota={} cliente={} produto={} quantidade={} valorTotal={}",
            notaFiscal.getNumeroNota(), notaFiscal.getCliente(), notaFiscal.getProduto(), notaFiscal.getQuantidade(), notaFiscal.getValorTotal());
  
        NotaFiscalEntity notaFiscalEntity = new NotaFiscalEntity();
        notaFiscalEntity.setNumeroNota(notaFiscal.getNumeroNota());
        notaFiscalEntity.setCliente(notaFiscal.getCliente());
        notaFiscalEntity.setProduto(notaFiscal.getProduto());
        notaFiscalEntity.setQuantidade(notaFiscal.getQuantidade());
        notaFiscalEntity.setValorTotal(notaFiscal.getValorTotal());

        log.info("[SERVICE-NOTA-FISCAL] Nota fiscal mapeada para entidade | numeroNota={} cliente={} produto={} quantidade={} valorTotal={}",
            notaFiscalEntity.getNumeroNota(), notaFiscalEntity.getCliente(), notaFiscalEntity.getProduto(), notaFiscalEntity.getQuantidade(), notaFiscalEntity.getValorTotal());

        int tentativa = 1;
        while (true) {
            try {
                log.info("[SERVICE-NOTA-FISCAL] Enviando nota fiscal para banco de dados | numeroNota={} tentativa={}",
                    notaFiscalEntity.getNumeroNota(), tentativa);

                NotaFiscalEntity notaFiscalSalva = notaFiscalRepository.save(notaFiscalEntity);

                log.info("[SERVICE-NOTA-FISCAL] Nota fiscal persistida com sucesso | id={} numeroNota={} cliente={} tentativa={} statusBanco=RECUPERADO",
                    notaFiscalSalva.getId(), notaFiscalSalva.getNumeroNota(), notaFiscalSalva.getCliente(), tentativa);

                return notaFiscalSalva;
            } catch (DataAccessException ex) {
                long proximaTentativaMs = calcularBackoffComJitter(tentativa);

                log.error("[SERVICE-NOTA-FISCAL] Falha ao persistir nota fiscal no banco | numeroNota={} tentativa={} proximaTentativaEmMs={} erro={}",
                    notaFiscalEntity.getNumeroNota(), tentativa, proximaTentativaMs, ex.getMessage());

                aguardarProximaTentativa(notaFiscalEntity.getNumeroNota(), tentativa, proximaTentativaMs);

                tentativa++;
            }
        }
    }


    public List<NotaFiscalEntity> listarNotasFiscais() {
        log.info("[SERVICE-NOTA-FISCAL] Consultando notas fiscais no banco de dados | operacao=listarNotasFiscais");
        List<NotaFiscalEntity> notasFiscais = notaFiscalRepository.findAll();
        log.info("[SERVICE-NOTA-FISCAL] Consulta de notas fiscais finalizada | totalNotasFiscais={}", notasFiscais.size());
        return notasFiscais;
    }

    private long calcularBackoffComJitter(int tentativaAtual) {
        long exponencial = retryIntervalMs * (1L << Math.min(tentativaAtual - 1, 10));
        long limitado = Math.min(exponencial, retryMaxIntervalMs);
        long jitter = ThreadLocalRandom.current().nextLong(250, 1000);
        return Math.min(limitado + jitter, retryMaxIntervalMs);
    }

    private void aguardarProximaTentativa(String numeroNota, int tentativaAtual, long esperaMs) {
        try {
            Thread.sleep(esperaMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "[SERVICE-NOTA-FISCAL] Thread interrompida durante espera de recuperação do banco | numeroNota="
                    + numeroNota + " tentativa=" + tentativaAtual + " esperaMs=" + esperaMs,
                ex
            );
        }
    }

}
