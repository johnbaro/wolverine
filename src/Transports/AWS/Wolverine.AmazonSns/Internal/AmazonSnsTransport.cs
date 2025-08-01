﻿using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using JasperFx.Core;
using Wolverine.AmazonSqs.Internal;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.AmazonSns.Internal;

public class AmazonSnsTransport : BrokerTransport<AmazonSnsTopic>
{
    public const string SnsProtocol = "sns";
    
    public const char Separator = '-';
    
    public AmazonSnsTransport() : base(SnsProtocol, "Amazon SNS")
    {
        Topics = new LightweightCache<string, AmazonSnsTopic>(name => new AmazonSnsTopic(name, this));
        
        IdentifierDelimiter = "-";
    }
    
    internal AmazonSnsTransport(IAmazonSimpleNotificationService snsClient, IAmazonSQS sqsClient) : this()
    {
        SnsClient = snsClient;
        SqsClient = sqsClient;
    }

    public override Uri ResourceUri => new(SnsConfig.ServiceURL);

    public Func<IWolverineRuntime, AWSCredentials>? CredentialSource { get; set; }
    public LightweightCache<string, AmazonSnsTopic> Topics { get; }
    
    public AmazonSimpleNotificationServiceConfig SnsConfig { get; } = new();
    internal IAmazonSimpleNotificationService? SnsClient { get; private set; }
    internal IAmazonSQS? SqsClient { get; private set; }

    public int LocalStackPort { get; set; }

    public bool UseLocalStackInDevelopment { get; set; }
    internal AmazonSqsTransport SQS { get; set; }

    // TODO duplicated code in SqsTransport
    public static string SanitizeSnsName(string identifier)
    {
        //AWS requires FIFO topics to have a `.fifo` suffix
        var suffixIndex = identifier.LastIndexOf(".fifo", StringComparison.OrdinalIgnoreCase);

        if (suffixIndex != -1) // ".fifo" suffix found
        {
            var prefix = identifier[..suffixIndex];
            var suffix = identifier[suffixIndex..];

            prefix = prefix.Replace('.', Separator);

            return prefix + suffix;
        }

        // ".fifo" suffix not found
        return identifier.Replace('.', Separator);
    }

    public override string SanitizeIdentifier(string identifier)
    {
        return SanitizeSnsName(identifier);
    }
    
    protected override IEnumerable<AmazonSnsTopic> endpoints()
    {
        return Topics;
    }

    protected override AmazonSnsTopic findEndpointByUri(Uri uri)
    {
        if (uri.Scheme != Protocol)
        {
            throw new ArgumentOutOfRangeException(nameof(uri));
        }
        
        return Topics.FirstOrDefault(x => x.Uri.OriginalString == uri.OriginalString) ?? Topics[uri.OriginalString.Split("//")[1].TrimEnd('/')];
    }

    public override ValueTask ConnectAsync(IWolverineRuntime runtime)
    {
        SnsClient ??= buildSnsClient(runtime);
        SqsClient ??= buildSqsClient(runtime);
        return ValueTask.CompletedTask;
    }

    public override IEnumerable<PropertyColumn> DiagnosticColumns()
    {
        throw new NotImplementedException();
    }
    
    internal AmazonSnsTopic EndpointForTopic(string topicName)
    {
        return Topics[topicName];
    }
    
    internal void ConnectToLocalStack(int port = 4566)
    {
        CredentialSource = _ => new BasicAWSCredentials("ignore", "ignore");
        SnsConfig.ServiceURL = $"http://localhost:{port}";
        SQS.Config.ServiceURL = $"http://localhost:{port}";
    }
    
    private IAmazonSimpleNotificationService buildSnsClient(IWolverineRuntime runtime)
    {
        if (CredentialSource == null)
        {
            return new AmazonSimpleNotificationServiceClient(SnsConfig);
        }

        var credentials = CredentialSource(runtime);
        return new AmazonSimpleNotificationServiceClient(credentials, SnsConfig);
    }
    
    private AmazonSQSClient buildSqsClient(IWolverineRuntime runtime)
    {
        if (CredentialSource == null)
        {
            return new AmazonSQSClient(SQS.Config);
        }

        var credentials = CredentialSource(runtime);
        return new AmazonSQSClient(credentials, SQS.Config);
    }

    /// <summary>
    /// Override to customize the queue policy for permissions for an AWS SQS queue that subscribes to
    /// an SNS topic
    /// </summary>
    public Func<SqsTopicDescription, string> QueuePolicyBuilder { get; set; } = description =>
    {
        var queuePolicy = $$"""
                            {
                              "Version": "2012-10-17",
                              "Statement": [{
                                  "Effect": "Allow",
                                  "Principal": {
                                      "Service": "sns.amazonaws.com"
                                  },
                                  "Action": "sqs:SendMessage",
                                  "Resource": "{{description.QueueArn}}",
                                  "Condition": {
                                    "ArnEquals": {
                                        "aws:SourceArn": "{{description.TopicArn}}"
                                    }
                                  }
                              }]
                            }
                            """;

        return queuePolicy;
    };
}
