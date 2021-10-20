import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "producer",
  brokers: ["broker:29092"],
});

(async function () {
  const producer = kafka.producer();

  await producer.connect();

  await Promise.all([
    producer.send({ topic: "test-topic", messages: [{ value: "First" }] }),
    producer.send({ topic: "test-topic", messages: [{ value: "Second" }] }),
    producer.send({ topic: "test-topic", messages: [{ value: "Third" }] }),
  ]);

  console.log("Sent!");

  await producer.disconnect();
})();
