using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using JasperFx.Resources;
using Shouldly;
using Wolverine.ComplianceTests;
using Wolverine.RabbitMQ.Internal;
using Wolverine.Runtime;
using Wolverine.Tracking;
using Xunit;
using Xunit.Abstractions;

namespace Wolverine.RabbitMQ.Tests;

public class end_to_end_with_named_broker
{
    private readonly ITestOutputHelper _output;
    private readonly BrokerName theName = new BrokerName("other");

    public end_to_end_with_named_broker(ITestOutputHelper output)
    {
        _output = output;
    }
    
    [Fact]
    public async Task send_message_to_and_receive_through_rabbitmq_with_inline_receivers()
    {
        var queueName = RabbitTesting.NextQueueName();
        using var publisher = WolverineHost.For(opts =>
        {
            opts.AddNamedRabbitMqBroker(theName, factory => {}).AutoProvision().AutoPurgeOnStartup();

            opts.PublishAllMessages()
                .ToRabbitQueueOnNamedBroker(theName, queueName)
                .SendInline();

            opts.Services.AddResourceSetupOnStartup(StartupAction.ResetState);
        });


        using var receiver = WolverineHost.For(opts =>
        {
            opts.AddNamedRabbitMqBroker(theName, factory => { }).AutoProvision();

            opts.ListenToRabbitQueueOnNamedBroker(theName, queueName).ProcessInline().Named(queueName);
            opts.Services.AddSingleton<ColorHistory>();

            opts.Services.AddResourceSetupOnStartup(StartupAction.ResetState);
        });

        await receiver.ResetResourceState();

        for (int i = 0; i < 10000; i++)
        {
            await publisher.SendAsync(new ColorChosen { Name = "blue" });
        }

        var cancellation = new CancellationTokenSource(30.Seconds());
        var queue = receiver.Get<IWolverineRuntime>().Endpoints.EndpointByName(queueName).ShouldBeOfType<RabbitMqQueue>();

        while (!cancellation.IsCancellationRequested && await queue.QueuedCountAsync() > 0)
        {
            await Task.Delay(250.Milliseconds(), cancellation.Token);
        }

        cancellation.Token.ThrowIfCancellationRequested();


    }

    [Fact]
    public async Task correct_scheme_on_reply_uri()
    {
        var queueName = RabbitTesting.NextQueueName();
        using var publisher = WolverineHost.For(opts =>
        {
            opts.Discovery.DisableConventionalDiscovery();
            
            opts.AddNamedRabbitMqBroker(theName, factory => {}).AutoProvision().AutoPurgeOnStartup();

            opts.PublishAllMessages()
                .ToRabbitQueueOnNamedBroker(theName, queueName)
                .SendInline();

            opts.Services.AddResourceSetupOnStartup(StartupAction.ResetState);
        });


        using var receiver = WolverineHost.For(opts =>
        {
            opts.UseRabbitMq().AutoProvision();

            opts.ListenToRabbitQueue(queueName).Named(queueName);
            
            opts.Services.AddSingleton<ColorHistory>();

            opts.Services.AddResourceSetupOnStartup(StartupAction.ResetState);
        });

        await receiver.ResetResourceState();

        var request = new RequestId(Guid.NewGuid());
        var (tracked, response) =
            await publisher.TrackActivity().AlsoTrack(receiver).InvokeAndWaitAsync<ResponseId>(request);
        
        response.Id.ShouldBe(request.Id);
        tracked.Received.SingleEnvelope<ResponseId>().Destination.Scheme.ShouldBe("other");
    }

}

public record RequestId(Guid Id);
public record ResponseId(Guid Id);

public static class RequestIdHandler
{
    public static ResponseId Handle(RequestId message) => new ResponseId(message.Id);
}