import { Kafka } from 'kafkajs';
import type { Producer, Consumer, RecordMetadata } from 'kafkajs';

export interface IKafkaService {
  sendMessageToTopic({ topic, value }): Promise<RecordMetadata[]>;
  subcribeMessageFromTopic(topic: string, callback: (value: any) => void);
}

export class KafkaService implements IKafkaService {
  producer: Producer;
  consumer: Consumer;

  constructor(setting: {
    brokers: string[];
    clientId: string;
    groupId: string;
  }) {
    const { brokers, clientId, groupId } = setting;

    const kafka = new Kafka({ clientId, brokers });
    this.producer = kafka.producer();
    this.consumer = kafka.consumer({ groupId });
  }

  async sendMessageToTopic({ topic, value }): Promise<RecordMetadata[]> {
    try {
      await this.producer.connect();

      const responses = await this.producer.send({
        topic: topic,
        messages: [{ value }],
      });
      await this.producer.disconnect();
      return responses;
    } catch (error) {
      console.error('Error trying to write data to Kafka', error);
    }
  }

  async subcribeMessageFromTopic(
    topic: string,
    callback: (value: any) => void
  ) {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic });
      await this.consumer.run({
        eachMessage: async ({ message }) => {
          try {
            callback(message);
          } catch (error) {
            console.log(error);
          }
        },
      });
    } catch (error) {
      console.error('Error trying to read data from Kafka', error);
    }
  }
}
