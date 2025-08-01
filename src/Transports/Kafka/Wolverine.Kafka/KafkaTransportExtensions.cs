using System.Text;
using Confluent.Kafka;
using JasperFx.Core.Reflection;
using Wolverine.Configuration;
using Wolverine.Kafka.Internals;

namespace Wolverine.Kafka;

public static class KafkaTransportExtensions
{
    /// <summary>
    ///     Quick access to the Kafka Transport within this application.
    ///     This is for advanced usage
    /// </summary>
    /// <param name="endpoints"></param>
    /// <returns></returns>
    internal static KafkaTransport KafkaTransport(this WolverineOptions endpoints, BrokerName? name = null)
    {
        var transports = endpoints.As<WolverineOptions>().Transports;

        return transports.GetOrCreate<KafkaTransport>(name);
    }

    /// <summary>
    /// Add a connection to an Kafka broker within this application
    /// </summary>
    /// <param name="options"></param>
    /// <param name="configure"></param>
    /// <returns></returns>
    public static KafkaTransportExpression UseKafka(this WolverineOptions options, string bootstrapServers)
    {
        // Automatic failure acks do not work with Kafka serialization failures
        options.EnableAutomaticFailureAcks = false;
        var transport = options.KafkaTransport();
        transport.ConsumerConfig.BootstrapServers = bootstrapServers;
        transport.ProducerConfig.BootstrapServers = bootstrapServers;
        transport.AdminClientConfig.BootstrapServers = bootstrapServers;

        return new KafkaTransportExpression(transport, options);
    }
    
    /// <summary>
    /// Configure connection and authentication information for a secondary Kafka broker
    /// to this application. Only use this overload if your Wolverine application needs to talk
    /// to two or more Kafka brokers
    /// </summary>
    /// <param name="options"></param>
    /// <param name="name">Name of the additional Rabbit Mq broker</param>
    /// <param name="configure"></param>
    public static KafkaTransportExpression AddNamedKafkaBroker(this WolverineOptions options, BrokerName name,
        string bootstrapServers)
    {
        var transport = options.KafkaTransport(name);
        transport.ConsumerConfig.BootstrapServers = bootstrapServers;
        transport.ProducerConfig.BootstrapServers = bootstrapServers;
        transport.AdminClientConfig.BootstrapServers = bootstrapServers;

        return new KafkaTransportExpression(transport, options);
    }

    /// <summary>
    /// Make additive configuration to the Kafka integration for this application
    /// </summary>
    /// <param name="options"></param>
    /// <param name="configure"></param>
    /// <returns></returns>
    public static KafkaTransportExpression ConfigureKafka(this WolverineOptions options, string bootstrapServers)
    {
        var transport = options.KafkaTransport();

        return new KafkaTransportExpression(transport, options);
    }

    /// <summary>
    ///     Listen for incoming messages at the designated Kafka topic name
    /// </summary>
    /// <param name="endpoints"></param>
    /// <param name="topicName">The name of the Kafka topic</param>
    /// <param name="configure">
    ///     Optional configuration for this Rabbit Mq queue if being initialized by Wolverine
    ///     <returns></returns>
    public static KafkaListenerConfiguration ListenToKafkaTopic(this WolverineOptions endpoints, string topicName)
    {
        var transport = endpoints.KafkaTransport();

        var endpoint = transport.Topics[topicName];
        endpoint.EndpointName = topicName;
        endpoint.IsListener = true;

        return new KafkaListenerConfiguration(endpoint);
    }
    
    /// <summary>
    ///     Listen for incoming messages at the designated Kafka topic name
    /// </summary>
    /// <param name="endpoints"></param>
    /// <param name="name">The name of the ancillary Kafka broker</param>
    /// <param name="topicName">The name of the Rabbit MQ queue</param>
    ///     <returns></returns>
    public static KafkaListenerConfiguration ListenToKafkaTopicOnNamedBroker(this WolverineOptions endpoints, BrokerName name, string topicName)
    {
        var transport = endpoints.KafkaTransport(name);

        var endpoint = transport.Topics[topicName];
        endpoint.EndpointName = topicName;
        endpoint.IsListener = true;

        return new KafkaListenerConfiguration(endpoint);
    }

    /// <summary>
    /// Publish messages to an Kafka topic
    /// </summary>
    /// <param name="publishing"></param>
    /// <param name="topicName"></param>
    /// <returns></returns>
    public static KafkaSubscriberConfiguration ToKafkaTopic(this IPublishToExpression publishing, string topicName)
    {
        var transports = publishing.As<PublishingExpression>().Parent.Transports;
        var transport = transports.GetOrCreate<KafkaTransport>();

        var topic = transport.Topics[topicName];

        // This is necessary unfortunately to hook up the subscription rules
        publishing.To(topic.Uri);

        return new KafkaSubscriberConfiguration(topic);
    }
    
    /// <summary>
    /// Publish messages to an Kafka topic
    /// </summary>
    /// <param name="publishing"></param>
    /// <param name="topicName"></param>
    /// <returns></returns>
    public static KafkaSubscriberConfiguration ToKafkaTopicOnNamedBroker(this IPublishToExpression publishing, BrokerName name, string topicName)
    {
        var transports = publishing.As<PublishingExpression>().Parent.Transports;
        var transport = transports.GetOrCreate<KafkaTransport>(name);

        var topic = transport.Topics[topicName];

        // This is necessary unfortunately to hook up the subscription rules
        publishing.To(topic.Uri);

        return new KafkaSubscriberConfiguration(topic);
    }

    /// <summary>
    /// Publish messages to Kafka topics based on Wolverine's rules for deriving topic
    /// names from a message type
    /// </summary>
    /// <param name="publishing"></param>
    /// <param name="topicName"></param>
    /// <returns></returns>
    public static KafkaSubscriberConfiguration ToKafkaTopics(this IPublishToExpression publishing)
    {
        var transports = publishing.As<PublishingExpression>().Parent.Transports;
        var transport = transports.GetOrCreate<KafkaTransport>();

        var topic = transport.Topics[KafkaTopic.WolverineTopicsName];

        // This is necessary unfortunately to hook up the subscription rules
        publishing.To(topic.Uri);

        return new KafkaSubscriberConfiguration(topic);
    }
    
    /// <summary>
    /// Publish messages to Kafka topics based on Wolverine's rules for deriving topic
    /// names from a message type
    /// </summary>
    /// <param name="publishing"></param>
    /// <param name="topicName"></param>
    /// <returns></returns>
    public static KafkaSubscriberConfiguration ToKafkaTopicsOnNamedBroker(this IPublishToExpression publishing, BrokerName name)
    {
        var transports = publishing.As<PublishingExpression>().Parent.Transports;
        var transport = transports.GetOrCreate<KafkaTransport>(name);

        var topic = transport.Topics[KafkaTopic.WolverineTopicsName];

        // This is necessary unfortunately to hook up the subscription rules
        publishing.To(topic.Uri);

        return new KafkaSubscriberConfiguration(topic);
    }

    internal static Envelope CreateEnvelope(this IKafkaEnvelopeMapper mapper, string topicName, Message<string, byte[]> message)
    {
        var envelope = new Envelope
        {
            PartitionKey = message.Key,
            Data = message.Value,
            TopicName = topicName
        };

        message.Headers ??= new Headers(); // prevent NRE

        mapper.MapIncomingToEnvelope(envelope, message);

        return envelope;
    }

    internal static async ValueTask<Message<string, byte[]>> CreateMessage(this IKafkaEnvelopeMapper mapper, Envelope envelope)
    {
        var data = await envelope.GetDataAsync();
        var message = new Message<string, byte[]>
        {
            Key = !string.IsNullOrEmpty(envelope.PartitionKey) ? envelope.PartitionKey : envelope.Id.ToString(),
            Value = data,
            Headers = new Headers()
        };

        mapper.MapEnvelopeToOutgoing(envelope, message);

        return message;
    }
}