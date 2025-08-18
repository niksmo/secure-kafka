# Secure-Kafka

Проект для отработки навыков шифрования соединения и аутентификации в Kafka.

Компоненты системы:

- http_handler - принимает данные в формате JSON
- kafka_producer - отправляет данные от хендлера в два топика
- kafka_consumer - получает сообщения из двух топиков

## Запуск проекта

1. Клонируйте репозиторий, установите зависимости:

```
git clone git@github.com:niksmo/secure-kafka.git
cd secure-kafka
go mod download
```

2. Запустите инфраструктуру используя `docker compose`:

```
docker compose up -d
```

3. Скомпилируйте Go-приложения:

```
go build -o secure-kafka-app ./cmd/.
```

4. Установите права для `producer` и `consumer` (консьюмеру доступен только `topic_1`):

```
docker compose exec kafka-1 kafka-acls --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin-client.properties \
  --add --allow-principal User:producer --producer --topic topic_1 --topic topic_2

docker compose exec kafka-1 kafka-acls --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin-client.properties \
  --add --allow-principal User:consumer --consumer --topic topic_1 --group group_1
```

5. Запустите приложение, важно чтобы папка `certs` была в той же директории, что и исполняемый файл приложения:

```
./secure-kafka-app
```

## Проверка функционала

1. Отправьте тестовые данные на эндпоинт `http://localhost:8000/v1/message`:

```
curl -i --header 'content-type: application/json' \
  --data '{"message": "hello, world!"}' \
  --url http://localhost:8000/v1/message
```

2. Убедитесь, что в терминале с запущенным приложением, присутствуют сообщения из примера:

```
# Producer
{"time":"...","level":"INFO","msg":"message produced","op":"KafkaProducer.produce","topic":"topic_2","partition":0}
{"time":"...","level":"INFO","msg":"message produced","op":"KafkaProducer.produce","topic":"topic_1","partition":0}

# Consumer
{"time":"...","level":"INFO","msg":"read message","op":"KafkaConsumer.handleTopics","topic":"topic_1","message":"{\"ID\":\"...\",\"Data\":{\"message\":\"hello, world!\"}}"}
{"time":"...","level":"WARN","msg":"authorization failed","op":"KafkaConsumer.getFetches","topic":"topic_2"}
```
