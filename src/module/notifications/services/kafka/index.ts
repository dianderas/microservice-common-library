import { Kafka } from 'kafkajs';
import type { Producer, Consumer, RecordMetadata } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

export interface IKafkaService {
  sendMessageToTopic({
    key,
    topic,
    encodePayloadId,
    payload,
  }): Promise<RecordMetadata[]>;
  subcribeMessageFromTopic(topic: string, callback: (value: any) => void);
  findSchemaBySubjectAndVersion(
    subject: string,
    version: string | number
  ): Promise<number>;
}

export class KafkaService implements IKafkaService {
  producer: Producer;
  consumer: Consumer;
  registry: SchemaRegistry;

  constructor(setting: {
    brokers: string[];
    host: string;
    clientId: string;
    groupId: string;
    username?: string;
    password?: string;
  }) {
    const { username, password, brokers, host, clientId, groupId } = setting;
    const sasl =
      username && password ? { username, password, mechanism: 'plain' } : null;
    const ssl = !!sasl;

    this.registry = new SchemaRegistry({ host });
    const kafka = new Kafka({ clientId, brokers /*ssl sasl*/ });
    this.producer = kafka.producer();
    this.consumer = kafka.consumer({ groupId });
  }

  async findSchemaBySubjectAndVersion(
    subject: string,
    version: string | number
  ): Promise<number> {
    return await this.registry.getRegistryId(subject, version);
  }

  async sendMessageToTopic({
    key,
    topic,
    encodePayloadId,
    payload,
  }): Promise<RecordMetadata[]> {
    try {
      await this.producer.connect();
      const encodedPayload = await this.registry.encode(
        encodePayloadId,
        payload
      );

      const responses = await this.producer.send({
        topic: topic,
        messages: [{ key, value: encodedPayload }],
      });

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
            const decodedMessage = {
              ...message,
              // key: await registry.decode(message.key),
              value: await this.registry.decode(message.value),
            };
            callback(decodedMessage);
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
