# kafka — Integrador Event-Driven

Integrador entre dois sistemas com arquitetura **Hexagonal + Orquestração via Event-Driven**.

## Stack

| Componente     | Tecnologia                          |
|----------------|-------------------------------------|
| Framework      | Spring Boot 3.3.4 + Java 21         |
| Mensageria     | Apache Kafka                        |
| Resiliência    | Resilience4j (CB + Retry + Bulkhead)|
| Métricas       | Prometheus + Grafana                |
| Logs           | Loki + Logback                      |
| Build          | Maven                               |
| Infraestrutura | Docker Compose                      |

---

## Estrutura do Projeto

```
kafka/
├── src/main/java/com/integracao/kafka/
│   ├── KafkaApplication.java
│   ├── adapter/
│   │   ├── in/
│   │   │   ├── kafka/KafkaConsumerAdapter.java  ← recebe do Kafka
│   │   │   └── http/PedidoController.java       ← recebe requisições HTTP
│   │   └── out/kafka/KafkaProducerAdapter.java  ← publica no Kafka
│   ├── application/usecase/
│   │   ├── ProcessarEventoUseCase.java          ← orquestrador de eventos
│   │   └── CriarPedidoUseCase.java              ← orquestrador de pedidos
│   ├── config/
│   │   └── KafkaConfig.java                     ← tópicos + DLQ
│   ├── domain/model/
│   │   ├── Evento.java
│   │   └── Pedido.java
│   ├── infrastructure/
│   │   └── metrics/IntegradorMetrics.java       ← métricas Prometheus
│   └── port/
│       ├── in/
│       │   ├── ProcessarEventoPort.java
│       │   └── CriarPedidoPort.java             ← porta para criar pedidos
│       └── out/PublicarEventoPort.java
├── src/main/resources/
│   ├── application.yaml
│   └── logback-spring.xml
├── docker/
│   ├── prometheus.yml
│   └── grafana/provisioning/
│       ├── datasources/datasources.yaml
│       └── dashboards/
│           ├── dashboards.yaml
│           └── kafka.json                       ← dashboard pronto
├── docker-compose.yml
└── pom.xml
```

---

## Como Rodar

### 1. Subir a infraestrutura (Kafka, Prometheus, Grafana, Loki)

```bash
docker-compose up -d
```

Aguarde ~30 segundos para o Kafka inicializar.

### 2. Compilar e rodar a aplicação

```bash
./mvnw spring-boot:run
```

Ou gerar o JAR e rodar:

```bash
./mvnw clean package -DskipTests
java -jar target/kafka-0.0.1-SNAPSHOT.jar
```

### 3. Verificar se está funcionando

```bash
# Health check
curl http://localhost:8080/actuator/health

# Métricas Prometheus
curl http://localhost:8080/actuator/prometheus | grep integrador
```

---

## Interfaces Disponíveis

| Interface       | URL                          | Credenciais      |
|-----------------|------------------------------|------------------|
| Aplicação       | http://localhost:8083        | —                |
| Swagger UI      | http://localhost:8083/swagger-ui.html | —       |
| Kafka UI        | http://localhost:8090        | —                |
| Prometheus      | http://localhost:9090        | —                |
| Grafana         | http://localhost:3000        | admin / admin    |
| Loki            | http://localhost:3100        | —                |

---

## Endpoints da API

### Pedidos

| Método | Endpoint             | Descrição                                    |
|--------|----------------------|----------------------------------------------|
| POST   | /api/pedidos         | Cria pedido e publica em entrada.pedido      |
| GET    | /api/pedidos/exemplo | Retorna exemplo de pedido com metadados mock|

### Health & Metrics

| Método | Endpoint                   | Descrição                              |
|--------|----------------------------|----------------------------------------|
| GET    | /api/ping                  | Health check simples                   |
| GET    | /actuator/health           | Health check completo (Spring)         |
| GET    | /actuator/prometheus       | Métricas no formato Prometheus         |

### Exemplo de Requisição

```bash
# Criar pedido
curl -X POST http://localhost:8083/api/pedidos \
  -H "Content-Type: application/json" \
  -d '{
    "numeroPedido": "PED-12345",
    "clienteId": "CLI-001",
    "produtos": [
      {
        "produtoId": "PROD-A",
        "quantidade": 2,
        "preco": 100.00
      }
    ],
    "valorTotal": 200.00
  }'

# Listar pedidos consumidos
curl http://localhost:8083/api/pedidos/kafka

# Buscar pedido específico
curl http://localhost:8083/api/pedidos/kafka/PED-12345
```

---

## Testando o Fluxo Completo

### Publicar uma mensagem de teste no Kafka

```bash
# Entrar no container do Kafka
docker exec -it kafka bash

# Publicar uma mensagem no tópico de entrada
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic entrada.evento

```

### Verificar no tópico de saída

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic saida.evento \
  --from-beginning
```

---

## Configurando Tópicos Personalizados

No `application.yaml`, adicione:

```yaml
integrador:
  topico:
    entrada: meu-topico-entrada
    saida: meu-topico-saida
```

---

## Grafana — Dashboard

1. Acesse http://localhost:3000 (admin/admin)
2. Vá em **Dashboards → kafka → kafka - Integrador**
3. O dashboard já está pré-configurado com:
   - Taxa de mensagens processadas (sucesso vs falha)
   - Latência P50 / P95 / P99
   - Estado dos Circuit Breakers
   - Contagem de retries
   - Logs em tempo real via Loki

---

## Fluxo de Resiliência

### Fluxo 1: Eventos Genéricos (entrada.evento → saida.evento)

```
Kafka (entrada.evento)
        │
        ▼
KafkaConsumerAdapter
  ├── Sucesso → ACK → segue fluxo
  ├── Erro de dados → ACK + descarta (sem retry)
  └── Erro de infra → sem ACK → Kafka reentrega
        │
        ▼ (após 3 falhas)
KafkaConfig.ErrorHandler
        │
        └── entrada.evento.DLQ
        │
        ▼
ProcessarEventoUseCase (orquestrador)
  1. Valida
  2. Transforma
  3. Publica via KafkaProducerAdapter
        │
KafkaProducerAdapter
  ├── Bulkhead → max 20 chamadas simultâneas
  ├── CircuitBreaker → abre se 30% falhar
  ├── Retry → 3-5 tentativas com backoff exponencial
  └── Fallback → Outbox Pattern (TODO)
        │
        ▼
Kafka (saida.evento)
        │
        ▼
     Sistema B
```

### Fluxo 2: Pedidos (entrada.pedido → saida.pedido)

```
Cliente HTTP
    │
    ▼ POST /api/pedidos
PedidoController (Adapter HTTP)
    │
    ▼ delega para use case
CriarPedidoUseCase (Orquestrador)
  1. Valida pedido (valorTotal > 0)
  2. Gera numeroPedido se vazio
  3. Preenche metadados mocados
  4. Encapsula em Evento
  5. Publica via PublicarEventoPort
    │
    ▼
KafkaProducerAdapter
  ├── Timeout: 30s (delivery)
  ├── Retry: 3 tentativas
  ├── CircuitBreaker + Bulkhead
  └── Publicação síncrona com confirmação
    │
    ▼
Kafka (entrada.pedido - 3 partições)
    │
    ▼
KafkaConsumerAdapter.consumirPedidoEntrada()
  1. Converte payload JSON → Pedido
  2. Valida numeroPedido != null
  3. Enriquece com metadados Kafka REAIS:
     - dataProcessamento (LocalDateTime)
     - statusProcessamento ("PROCESSADO")
     - kafkaOffset (offset real)
     - kafkaPartition (0-2)
     - kafkaTopic ("entrada.pedido")
     - kafkaTimestamp (Instant UTC)
  4. Cria Evento de saída (tipo=PEDIDO_PROCESSADO)
  5. Publica em saida.pedido via PublicarEventoPort
  6. ACK manual
    │
    ▼
Kafka (saida.pedido - 3 partições)
    │
    ▼
  Sistema Downstream
  (pode ser outro consumer, banco, etc.)
```

---

## Variáveis de Ambiente (Produção)

```bash
KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
LOKI_URL=http://loki:3100/loki/api/v1/push
APP_ENV=prod
SPRING_PROFILES_ACTIVE=prod
```
