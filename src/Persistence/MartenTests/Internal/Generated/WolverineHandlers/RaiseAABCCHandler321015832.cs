// <auto-generated/>
#pragma warning disable
using Wolverine.Marten.Publishing;

namespace Internal.Generated.WolverineHandlers
{
    // START: RaiseAABCCHandler321015832
    [global::System.CodeDom.Compiler.GeneratedCode("JasperFx", "1.0.0")]
    public sealed class RaiseAABCCHandler321015832 : Wolverine.Runtime.Handlers.MessageHandler
    {
        private readonly Wolverine.Marten.Publishing.OutboxedSessionFactory _outboxedSessionFactory;

        public RaiseAABCCHandler321015832(Wolverine.Marten.Publishing.OutboxedSessionFactory outboxedSessionFactory)
        {
            _outboxedSessionFactory = outboxedSessionFactory;
        }



        public override async System.Threading.Tasks.Task HandleAsync(Wolverine.Runtime.MessageContext context, System.Threading.CancellationToken cancellation)
        {
            // The actual message body
            var raiseAABCC = (MartenTests.AggregateHandlerWorkflow.RaiseAABCC)context.Envelope.Message;

            System.Diagnostics.Activity.Current?.SetTag("message.handler", "MartenTests.AggregateHandlerWorkflow.RaiseLetterHandler");
            await using var documentSession = _outboxedSessionFactory.OpenSession(context);
            var eventStoreOperations = documentSession.Events;
            var aggregateId = raiseAABCC.LetterAggregateId;
            
            // Loading Marten aggregate
            var eventStream = await eventStoreOperations.FetchForWriting<MartenTests.AggregateHandlerWorkflow.LetterAggregate>(aggregateId, cancellation).ConfigureAwait(false);

            
            // The actual message execution
            (var outgoing1, var outgoing2) = MartenTests.AggregateHandlerWorkflow.RaiseLetterHandler.Handle(raiseAABCC, eventStream.Aggregate);

            
            // Outgoing, cascaded message
            await context.EnqueueCascadingAsync(outgoing1).ConfigureAwait(false);

            if (outgoing2 != null)
            {
                
                // Capturing any possible events returned from the command handlers
                eventStream.AppendMany(outgoing2);

            }

            await documentSession.SaveChangesAsync(cancellation).ConfigureAwait(false);
        }

    }

    // END: RaiseAABCCHandler321015832
    
    
}

