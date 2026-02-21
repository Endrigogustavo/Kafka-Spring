package com.integracao.kafka.application.useCase.subscribe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.integracao.kafka.domain.entity.NotaFiscal;

@Service
public class ReceberNotaUseCase {

	private final int limiteHistorico;
	private final ConcurrentLinkedDeque<NotaFiscal> historico = new ConcurrentLinkedDeque<>();

	public ReceberNotaUseCase(@Value("${integrador.historico.notas.limite:500}") int limiteHistorico) {
		this.limiteHistorico = Math.max(1, limiteHistorico);
	}

	public void registrar(NotaFiscal notaFiscal) {
		if (notaFiscal == null) {
			return;
		}

		historico.addLast(notaFiscal);

		while (historico.size() > limiteHistorico) {
			historico.pollFirst();
		}
	}

	public List<NotaFiscal> listarUltimas(int limite) {
		List<NotaFiscal> itens = new ArrayList<>(historico);
		if (itens.isEmpty()) {
			return List.of();
		}

		int limiteAjustado = limite <= 0 ? itens.size() : Math.min(limite, itens.size());
		int inicio = Math.max(0, itens.size() - limiteAjustado);
		return List.copyOf(itens.subList(inicio, itens.size()));
	}
}
