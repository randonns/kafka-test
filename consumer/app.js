import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "consumer",
  brokers: ["broker:29092"],
});

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

(async function () {
  const consumer = kafka.consumer({ groupId: "test-group" });

  await consumer.connect();
  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        new Date(),
        topic,
        partition,
        message.value.toString(),
        "START"
      );
      await delay(2000);
      console.log(
        new Date(),
        topic,
        partition,
        message.value.toString(),
        "DONE"
      );
    },
  });
})();
