﻿using JasperFx;
using JasperFx.Core;
using JasperFx.Core.IoC;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Subscriptions;
using Marten;
using Marten.Events.Daemon.Coordination;
using Marten.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Wolverine.Marten.Distribution;
using Wolverine.Marten.Publishing;
using Wolverine.Marten.Subscriptions;
using Wolverine.Persistence.Durability;
using Wolverine.Postgresql;
using Wolverine.RDBMS;
using Wolverine.RDBMS.MultiTenancy;
using Wolverine.Runtime;
using Wolverine.Runtime.Agents;
using MultiTenantedMessageStore = Wolverine.Persistence.Durability.MultiTenantedMessageStore;

namespace Wolverine.Marten;

internal class MapEventTypeMessages : IWolverineExtension
{
    public void Configure(WolverineOptions options)
    {
        options.MapGenericMessageType(typeof(IEvent<>), typeof(Event<>));
    }
}

public static class WolverineOptionsMartenExtensions
{
    /// <summary>
    ///     Integrate Marten with Wolverine's persistent outbox and add Marten-specific middleware
    ///     to Wolverine
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="schemaName">Optionally move the Wolverine envelope storage to a separate schema</param>
    /// <param name="masterDataSource">
    ///     In the case of Marten using a database per tenant, you may wish to
    ///     explicitly determine the master database for Wolverine where Wolverine will store node and envelope information.
    ///     This does not have to be one of the tenant databases
    ///     Wolverine will try to use the master database from the Marten configuration when possible
    /// </param>
    /// <param name="masterDatabaseConnectionString">
    ///     In the case of Marten using a database per tenant, you may wish to
    ///     explicitly determine the master database for Wolverine where Wolverine will store node and envelope information.
    ///     This does not have to be one of the tenant databases
    ///     Wolverine will try to use the master database from the Marten configuration when possible
    /// </param>
    /// <param name="transportSchemaName">Optionally configure the schema name for any PostgreSQL queues</param>
    /// <param name="autoCreate">
    ///     Optionally override whether to automatically create message database schema objects. Defaults
    ///     to <see cref="StoreOptions.AutoCreateSchemaObjects" />.
    /// </param>
    /// <returns></returns>
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression IntegrateWithWolverine(
        this MartenServiceCollectionExtensions.MartenConfigurationExpression expression,
        Action<MartenIntegration>? configure = null)
    {
        var integration = expression.Services.FindMartenIntegration();
        if (integration == null)
        {
            integration = new MartenIntegration();

            configure?.Invoke(integration);

            expression.Services.AddSingleton<IWolverineExtension>(integration);
        }
        else
        {
            configure?.Invoke(integration);
        }

        expression.Services.AddSingleton<IWolverineExtension, MapEventTypeMessages>();

        expression.Services.AddScoped<IMartenOutbox, MartenOutbox>();

        // Gotta have at least a placeholder just in case a user also has
        // EF Core
        expression.Services.AddSingleton<DatabaseSettings>(s =>
        {
            var store = s.GetRequiredService<IMessageStore>() as IMessageDatabase;
            if (store != null) return store.Settings;

            return new DatabaseSettings();
        });

        expression.Services.AddSingleton<IMessageStore>(s =>
        {
            var store = s.GetRequiredService<IDocumentStore>().As<DocumentStore>();

            var runtime = s.GetRequiredService<IWolverineRuntime>();
            var logger = s.GetRequiredService<ILogger<PostgresqlMessageStore>>();

            var schemaName = integration.MessageStorageSchemaName ??
                             runtime.Options.Durability.MessageStorageSchemaName ??
                             store.Options.DatabaseSchemaName ?? "public";


            if (store.Tenancy.Cardinality == DatabaseCardinality.Single)
            {
                return BuildSinglePostgresqlMessageStore(schemaName, integration.AutoCreate, store, runtime, logger);
            }

            var masterDatabaseConnectionString = integration.MasterDatabaseConnectionString;
            var masterDataSource = integration.MasterDataSource;

            if (store.Tenancy is MasterTableTenancy masterTableTenancy)
            {
                masterDataSource = masterTableTenancy.TenantDatabase.DataSource;
            }
            
            return BuildMultiTenantedMessageDatabase(schemaName, integration.AutoCreate,
                masterDatabaseConnectionString, masterDataSource, store, runtime, s);
        });

        if (integration.UseWolverineManagedEventSubscriptionDistribution)
        {
            expression.Services.AddSingleton<ProjectionAgents>();
            expression.Services.AddSingleton<IAgentFamily>(s => s.GetRequiredService<ProjectionAgents>());
            expression.Services.AddSingleton<IProjectionCoordinator>(s => s.GetRequiredService<ProjectionAgents>());
        }

        expression.Services.AddType(typeof(IDatabaseSource), typeof(MessageDatabaseDiscovery),
            ServiceLifetime.Singleton);

        expression.Services.AddSingleton<IConfigureMarten, MartenOverrides>();

        expression.Services.AddSingleton<OutboxedSessionFactory>();

        return expression;
    }

    internal static NpgsqlDataSource findMasterDataSource(
        DocumentStore store,
        IWolverineRuntime runtime,
        DatabaseSettings masterSettings,
        IServiceProvider container)
    {
        if (store.Tenancy is ITenancyWithMasterDatabase m)
        {
            return m.TenantDatabase.DataSource;
        }

        if (masterSettings.DataSource != null)
        {
            return (NpgsqlDataSource)masterSettings.DataSource;
        }

        if (masterSettings.ConnectionString.IsNotEmpty())
        {
            return NpgsqlDataSource.Create(masterSettings.ConnectionString);
        }

        var source = container.GetService<NpgsqlDataSource>();

        return source ??
               throw new InvalidOperationException(
                   "There is no configured connectivity for the required master PostgreSQL message database");
    }

    internal static IMessageStore BuildMultiTenantedMessageDatabase(
        string schemaName,
        AutoCreate? autoCreate,
        string? masterDatabaseConnectionString,
        NpgsqlDataSource? masterDataSource,
        DocumentStore store,
        IWolverineRuntime runtime,
        IServiceProvider serviceProvider)
    {
        var masterSettings = new DatabaseSettings
        {
            SchemaName = schemaName,
            AutoCreate = autoCreate ?? store.Options.AutoCreateSchemaObjects,
            IsMain = true,
            CommandQueuesEnabled = true,
            DataSource = masterDataSource ?? NpgsqlDataSource.Create(masterDatabaseConnectionString)
        };

        var dataSource = findMasterDataSource(store, runtime, masterSettings, serviceProvider);
        var main = new PostgresqlMessageStore(masterSettings, runtime.Options.Durability, dataSource,
            runtime.LoggerFactory.CreateLogger<PostgresqlMessageStore>())
        {
            Name = "Main"
        };


        var source = new MartenMessageDatabaseSource(schemaName, autoCreate ?? store.Options.AutoCreateSchemaObjects,
            store, runtime);

        main.Initialize(runtime);

        return new MultiTenantedMessageStore(main, runtime, source);
    }

    internal static IMessageStore BuildSinglePostgresqlMessageStore(
        string schemaName,
        AutoCreate? autoCreate,
        DocumentStore store,
        IWolverineRuntime runtime,
        ILogger<PostgresqlMessageStore> logger)
    {
        var settings = new DatabaseSettings
        {
            SchemaName = schemaName,
            AutoCreate = autoCreate ?? store.Options.AutoCreateSchemaObjects,
            IsMain = true,
            ScheduledJobLockId = $"{schemaName ?? "public"}:scheduled-jobs".GetDeterministicHashCode()
        };

        var dataSource = store.Storage.Database.As<PostgresqlDatabase>().DataSource;

        return new PostgresqlMessageStore(settings, runtime.Options.Durability, dataSource, logger);
    }

    internal static MartenIntegration? FindMartenIntegration(this IServiceCollection services)
    {
        var descriptor = services.FirstOrDefault(x =>
            x.ServiceType == typeof(IWolverineExtension) && x.ImplementationInstance is MartenIntegration);

        return descriptor?.ImplementationInstance as MartenIntegration;
    }

    /// <summary>
    ///     Enable publishing of events to Wolverine message routing when captured in Marten sessions that are enrolled in a
    ///     Wolverine outbox
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    [Obsolete(
        $"Favor using the {nameof(MartenIntegration.UseFastEventForwarding)} property as part of {nameof(IntegrateWithWolverine)}. This will be removed in Wolverine 4.0")]
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression EventForwardingToWolverine(
        this MartenServiceCollectionExtensions.MartenConfigurationExpression expression)
    {
        var integration = expression.Services.FindMartenIntegration();
        if (integration == null)
        {
            expression.IntegrateWithWolverine();
            integration = expression.Services.FindMartenIntegration();
        }

        integration!.UseFastEventForwarding = true;

        return expression;
    }

    /// <summary>
    ///     Enable publishing of events to Wolverine message routing when captured in Marten sessions that are enrolled in a
    ///     Wolverine outbox. This requires usage of Marten transactional middleware within Wolverine, and makes no guarantees
    ///     about ordering
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    [Obsolete(
        $"All of these options are available within the {nameof(IntegrateWithWolverine)}() method. This will be removed in Wolverine 4.0")]
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression EventForwardingToWolverine(
        this MartenServiceCollectionExtensions.MartenConfigurationExpression expression,
        Action<IEventForwarding> configure)
    {
        var integration = expression.Services.FindMartenIntegration();
        if (integration == null)
        {
            expression.IntegrateWithWolverine();
            integration = expression.Services.FindMartenIntegration();
        }

        integration!.UseFastEventForwarding = true;

        configure(integration);

        return expression;
    }

    /// <summary>
    ///     Register a custom subscription that will process a batch of Marten events at a time with
    ///     a user defined action
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="subscription"></param>
    /// <returns></returns>
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression SubscribeToEvents(
        this MartenServiceCollectionExtensions.MartenConfigurationExpression expression,
        IWolverineSubscription subscription)
    {
        expression.Services.SubscribeToEvents(subscription);
        return expression;
    }

    /// <summary>
    ///     Register a custom subscription that will process a batch of Marten events at a time with
    ///     a user defined action
    /// </summary>
    /// <param name="services"></param>
    /// <param name="subscription"></param>
    /// <returns></returns>
    public static IServiceCollection SubscribeToEvents(this IServiceCollection services,
        IWolverineSubscription subscription)
    {
        services.ConfigureMarten((sp, opts) =>
        {
            var runtime = sp.GetRequiredService<IWolverineRuntime>();
            opts.Projections.Subscribe(new WolverineSubscriptionRunner(subscription, runtime));
        });

        return services;
    }

    /// <summary>
    ///     Register a custom subscription that will process a batch of Marten events at a time with
    ///     a user defined action
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="lifetime">
    ///     Service lifetime of the subscription class within the application's IoC container
    ///     <returns></returns>
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression SubscribeToEventsWithServices<T>(
        this MartenServiceCollectionExtensions.MartenConfigurationExpression expression, ServiceLifetime lifetime)
        where T : class, IWolverineSubscription
    {
        expression.Services.SubscribeToEventsWithServices<T>(lifetime);

        return expression;
    }

    /// <summary>
    ///     <param name="expression"></param>
    ///     <param name="lifetime">Service lifetime of the subscription class within the application's IoC container
    /// </summary>
    /// <param name="lifetime"></param>
    /// <param name="services"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static IServiceCollection SubscribeToEventsWithServices<T>(this IServiceCollection services,
        ServiceLifetime lifetime)
        where T : class, IWolverineSubscription
    {
        switch (lifetime)
        {
            case ServiceLifetime.Singleton:
                services.AddSingleton<T>();
                services.ConfigureMarten((sp, opts) =>
                {
                    var subscription = sp.GetRequiredService<T>();
                    var runtime = sp.GetRequiredService<IWolverineRuntime>();
                    opts.Projections.Subscribe(new WolverineSubscriptionRunner(subscription, runtime));
                });
                break;

            default:
                services.AddScoped<T>();
                services.ConfigureMarten((sp, opts) =>
                {
                    var runtime = sp.GetRequiredService<IWolverineRuntime>();
                    opts.Projections.Subscribe(new ScopedWolverineSubscriptionRunner<T>(sp, runtime));
                });
                break;
        }

        return services;
    }

    /// <summary>
    ///     Create a subscription for Marten events to be processed in strict order by Wolverine
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="subscriptionName">Descriptive name for this event subscription for tracking with Marten</param>
    /// <param name="configure">Fine tune the asynchronous daemon behavior of this subscription</param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression
        ProcessEventsWithWolverineHandlersInStrictOrder(
            this MartenServiceCollectionExtensions.MartenConfigurationExpression expression,
            string subscriptionName, Action<ISubscriptionOptions>? configure = null)
    {
        expression.Services.ProcessEventsWithWolverineHandlersInStrictOrder(subscriptionName, configure);

        return expression;
    }

    /// <summary>
    ///     Create a subscription for Marten events to be processed in strict order by Wolverine
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="subscriptionName">Descriptive name for this event subscription for tracking with Marten</param>
    /// <param name="configure">Fine tune the asynchronous daemon behavior of this subscription</param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public static IServiceCollection ProcessEventsWithWolverineHandlersInStrictOrder(this IServiceCollection services,
        string subscriptionName, Action<ISubscriptionOptions>? configure)
    {
        if (subscriptionName.IsEmpty())
        {
            throw new ArgumentNullException(nameof(subscriptionName));
        }

        services.ConfigureMarten((sp, opts) =>
        {
            var runtime = sp.GetRequiredService<IWolverineRuntime>();

            var invoker = new InlineInvoker(subscriptionName, runtime);
            var subscription = new WolverineSubscriptionRunner(invoker, runtime);

            configure?.Invoke(subscription);

            opts.Projections.Subscribe(subscription);
        });

        return services;
    }

    /// <summary>
    ///     Relay events captured by Marten to Wolverine message publishing
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="subscriptionName">Descriptive name for this event subscription for tracking with Marten</param>
    /// <param name="configure">Fine tune the asynchronous daemon behavior of this subscription</param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public static MartenServiceCollectionExtensions.MartenConfigurationExpression PublishEventsToWolverine(
        this MartenServiceCollectionExtensions.MartenConfigurationExpression expression,
        string subscriptionName, Action<IPublishingRelay>? configure = null)
    {
        expression.Services.PublishEventsToWolverine(subscriptionName, configure);

        return expression;
    }

    /// <summary>
    ///     Relay events captured by Marten to Wolverine message publishing
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="subscriptionName">Descriptive name for this event subscription for tracking with Marten</param>
    /// <param name="configure">Fine tune the asynchronous daemon behavior of this subscription</param>
    /// <exception cref="ArgumentNullException"></exception>
    public static IServiceCollection PublishEventsToWolverine(this IServiceCollection services, string subscriptionName,
        Action<IPublishingRelay>? configure)
    {
        if (subscriptionName.IsEmpty())
        {
            throw new ArgumentNullException(nameof(subscriptionName));
        }

        services.ConfigureMarten((sp, opts) =>
        {
            var runtime = sp.GetRequiredService<IWolverineRuntime>();

            var relay = new PublishingRelay(subscriptionName);
            configure?.Invoke(relay);

            var subscription = new WolverineSubscriptionRunner(relay, runtime);

            opts.Projections.Subscribe(subscription);
        });

        return services;
    }
}