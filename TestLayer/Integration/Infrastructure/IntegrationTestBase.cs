using ApplicationLayer;
using Dapper;
using DataLayer;
using DataLayer.Dapper;
using InfrastructureLayer;
using MediatR;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Respawn;

namespace Integration.Infrastructure;

/// <summary>
/// Base class for integration tests providing common functionality.
/// </summary>
public abstract class IntegrationTestBase : IAsyncLifetime
{
    protected readonly IServiceProvider ServiceProvider;
    protected readonly IServiceScope Scope;
    protected readonly IMediator Mediator;
    protected readonly DomainLayer.Interfaces.Persistence.IUnitOfWork UnitOfWork;
    protected readonly ApplicationDbContext DbContext;
    private static Respawner? _respawner;
    private static readonly object _lock = new();
    private const string ConnectionString = "Server=(localdb)\\MSSQLLocalDB;Database=ExchangeRateUpdaterTest;Integrated Security=true;Connection Timeout=30;MultipleActiveResultSets=true;";

    protected IntegrationTestBase()
    {
        // Register Dapper TypeHandlers before any database operations
        SqlMapper.AddTypeHandler(new DateOnlyTypeHandler());

        // Build service collection manually
        var services = new ServiceCollection();

        // Register logging
        services.AddLogging(configure => configure.AddConsole());

        // Register DbContext WITHOUT retry logic (conflicts with manual transactions)
        services.AddDbContext<ApplicationDbContext>(options =>
            options.UseSqlServer(ConnectionString));

        // Register DataLayer services
        services.AddScoped<DataLayer.IUnitOfWork, DataLayer.UnitOfWork>();
        services.AddScoped<DataLayer.Repositories.IUserRepository, DataLayer.Repositories.UserRepository>();
        services.AddScoped<DataLayer.Repositories.ICurrencyRepository, DataLayer.Repositories.CurrencyRepository>();
        services.AddScoped<DataLayer.Repositories.IExchangeRateProviderRepository, DataLayer.Repositories.ExchangeRateProviderRepository>();
        services.AddScoped<DataLayer.Repositories.IExchangeRateRepository, DataLayer.Repositories.ExchangeRateRepository>();

        // Register Dapper services
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:DefaultConnection"] = ConnectionString
            })
            .Build();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddSingleton<DataLayer.Dapper.IDapperContext, DataLayer.Dapper.DapperContext>();
        services.AddScoped<DataLayer.Dapper.IStoredProcedureService, DataLayer.Dapper.StoredProcedureService>();
        services.AddScoped<DataLayer.Dapper.IViewQueryService, DataLayer.Dapper.ViewQueryService>();

        // Register InfrastructureLayer services
        services.AddScoped<DomainLayer.Interfaces.Persistence.IUnitOfWork, InfrastructureLayer.Persistence.DomainUnitOfWork>();
        services.AddScoped<DomainLayer.Interfaces.Repositories.IUserRepository, InfrastructureLayer.Persistence.Adapters.UserRepositoryAdapter>();
        services.AddScoped<DomainLayer.Interfaces.Repositories.ICurrencyRepository, InfrastructureLayer.Persistence.Adapters.CurrencyRepositoryAdapter>();
        services.AddScoped<DomainLayer.Interfaces.Repositories.IExchangeRateProviderRepository, InfrastructureLayer.Persistence.Adapters.ExchangeRateProviderRepositoryAdapter>();
        services.AddScoped<DomainLayer.Interfaces.Repositories.IExchangeRateRepository, InfrastructureLayer.Persistence.Adapters.ExchangeRateRepositoryAdapter>();
        services.AddScoped<DomainLayer.Interfaces.Services.IPasswordHasher, InfrastructureLayer.Services.BCryptPasswordHasher>();
        services.AddSingleton<DomainLayer.Interfaces.Services.IDateTimeProvider, InfrastructureLayer.ExternalServices.DateTimeProvider>();
        services.AddScoped<DomainLayer.Interfaces.Queries.ISystemViewQueries, InfrastructureLayer.Persistence.Adapters.ViewQueryRepositoryAdapter>();

        // Register Fake BackgroundJobService for testing (avoids Hangfire dependency)
        services.AddScoped<ApplicationLayer.Common.Interfaces.IBackgroundJobService, FakeBackgroundJobService>();
        services.AddScoped<ApplicationLayer.Common.Interfaces.IBackgroundJobScheduler, FakeBackgroundJobScheduler>();

        // Register ApplicationLayer with MediatR
        services.AddApplicationLayer();

        // Build service provider
        ServiceProvider = services.BuildServiceProvider();
        Scope = ServiceProvider.CreateScope();

        // Get services
        Mediator = Scope.ServiceProvider.GetRequiredService<IMediator>();
        UnitOfWork = Scope.ServiceProvider.GetRequiredService<DomainLayer.Interfaces.Persistence.IUnitOfWork>();
        DbContext = Scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
    }

    /// <summary>
    /// Called before each test. Ensures database is created and clean.
    /// </summary>
    public virtual async Task InitializeAsync()
    {
        // Ensure database is created
        await DbContext.Database.EnsureCreatedAsync();

        // Create database views (EF doesn't create views, only tables)
        await EnsureViewsCreatedAsync();

        // Initialize Respawn once
        if (_respawner == null)
        {
            lock (_lock)
            {
                if (_respawner == null)
                {
                    using var connection = new SqlConnection(ConnectionString);
                    connection.Open();

                    _respawner = Respawner.CreateAsync(connection, new RespawnerOptions
                    {
                        TablesToIgnore = new Respawn.Graph.Table[] { "__EFMigrationsHistory" },
                        DbAdapter = DbAdapter.SqlServer
                    }).GetAwaiter().GetResult();
                }
            }
        }

        // Reset database to clean state
        using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await _respawner!.ResetAsync(conn);
    }

    /// <summary>
    /// Creates database views that are not managed by Entity Framework.
    /// </summary>
    private async Task EnsureViewsCreatedAsync()
    {
        using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();

        // Create vw_AllLatestExchangeRates
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_AllLatestExchangeRates', 'V') IS NOT NULL
                DROP VIEW dbo.vw_AllLatestExchangeRates;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_AllLatestExchangeRates]
            AS
            WITH RankedRates AS (
                SELECT
                    er.Id,
                    er.ProviderId,
                    er.BaseCurrencyId,
                    er.TargetCurrencyId,
                    er.Rate,
                    er.Multiplier,
                    er.ValidDate,
                    er.Created,
                    er.Modified,
                    ROW_NUMBER() OVER (
                        PARTITION BY er.BaseCurrencyId, er.TargetCurrencyId
                        ORDER BY er.ValidDate DESC, er.Created DESC
                    ) AS RowNum
                FROM [dbo].[ExchangeRate] er
            )
            SELECT
                rr.Id,
                rr.ProviderId,
                p.Code AS ProviderCode,
                p.Name AS ProviderName,
                bc.Code AS BaseCurrencyCode,
                rr.BaseCurrencyId,
                tc.Code AS TargetCurrencyCode,
                rr.TargetCurrencyId,
                rr.Rate,
                rr.Multiplier,
                CAST(rr.Rate AS DECIMAL(19,6)) / NULLIF(rr.Multiplier, 0) AS RatePerUnit,
                rr.ValidDate,
                rr.Created,
                rr.Modified,
                DATEDIFF(DAY, rr.ValidDate, CAST(GETDATE() AS DATE)) AS DaysOld,
                CASE
                    WHEN rr.ValidDate = CAST(GETDATE() AS DATE) THEN 'Current'
                    WHEN DATEDIFF(DAY, rr.ValidDate, CAST(GETDATE() AS DATE)) <= 1 THEN 'Recent'
                    WHEN DATEDIFF(DAY, rr.ValidDate, CAST(GETDATE() AS DATE)) <= 7 THEN 'Week Old'
                    ELSE 'Outdated'
                END AS FreshnessStatus
            FROM RankedRates rr
            INNER JOIN [dbo].[ExchangeRateProvider] p ON rr.ProviderId = p.Id
            INNER JOIN [dbo].[Currency] bc ON rr.BaseCurrencyId = bc.Id
            INNER JOIN [dbo].[Currency] tc ON rr.TargetCurrencyId = tc.Id
            WHERE rr.RowNum = 1
              AND p.IsActive = 1;");

        // Create vw_AllLatestUpdatedExchangeRates
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_AllLatestUpdatedExchangeRates', 'V') IS NOT NULL
                DROP VIEW dbo.vw_AllLatestUpdatedExchangeRates;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_AllLatestUpdatedExchangeRates]
            AS
            WITH RankedRates AS (
                SELECT
                    er.Id,
                    er.ProviderId,
                    er.BaseCurrencyId,
                    er.TargetCurrencyId,
                    er.Rate,
                    er.Multiplier,
                    er.ValidDate,
                    er.Created,
                    er.Modified,
                    ROW_NUMBER() OVER (
                        PARTITION BY er.BaseCurrencyId, er.TargetCurrencyId
                        ORDER BY er.Created DESC
                    ) AS RowNum
                FROM [dbo].[ExchangeRate] er
            )
            SELECT
                rr.Id,
                rr.ProviderId,
                p.Code AS ProviderCode,
                p.Name AS ProviderName,
                bc.Code AS BaseCurrencyCode,
                rr.BaseCurrencyId,
                tc.Code AS TargetCurrencyCode,
                rr.TargetCurrencyId,
                rr.Rate,
                rr.Multiplier,
                CAST(rr.Rate AS DECIMAL(19,6)) / NULLIF(rr.Multiplier, 0) AS RatePerUnit,
                rr.ValidDate,
                rr.Created,
                rr.Modified,
                DATEDIFF(DAY, rr.ValidDate, CAST(GETDATE() AS DATE)) AS DaysOld,
                DATEDIFF(MINUTE, rr.Created, GETDATE()) AS MinutesSinceUpdate,
                CASE
                    WHEN DATEDIFF(MINUTE, rr.Created, GETDATE()) <= 60 THEN 'Fresh'
                    WHEN DATEDIFF(HOUR, rr.Created, GETDATE()) <= 24 THEN 'Recent'
                    WHEN DATEDIFF(DAY, rr.Created, GETDATE()) <= 7 THEN 'Week Old'
                    ELSE 'Outdated'
                END AS UpdateFreshness
            FROM RankedRates rr
            INNER JOIN [dbo].[ExchangeRateProvider] p ON rr.ProviderId = p.Id
            INNER JOIN [dbo].[Currency] bc ON rr.BaseCurrencyId = bc.Id
            INNER JOIN [dbo].[Currency] tc ON rr.TargetCurrencyId = tc.Id
            WHERE rr.RowNum = 1
              AND p.IsActive = 1;");

        // Create vw_CurrentExchangeRates
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_CurrentExchangeRates', 'V') IS NOT NULL
                DROP VIEW dbo.vw_CurrentExchangeRates;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_CurrentExchangeRates]
            AS
            SELECT
                er.Id,
                p.Id AS ProviderId,
                p.Code AS ProviderCode,
                p.Name AS ProviderName,
                bc.Code AS BaseCurrencyCode,
                tc.Code AS TargetCurrencyCode,
                tc.Id AS TargetCurrencyId,
                er.Rate,
                er.Multiplier,
                CAST(er.Rate AS DECIMAL(19,6)) / NULLIF(er.Multiplier, 0) AS RatePerUnit,
                er.ValidDate,
                er.Created
            FROM [dbo].[ExchangeRate] er
            INNER JOIN [dbo].[Currency] bc ON er.BaseCurrencyId = bc.Id
            INNER JOIN [dbo].[Currency] tc ON er.TargetCurrencyId = tc.Id
            INNER JOIN [dbo].[ExchangeRateProvider] p ON er.ProviderId = p.Id
            WHERE er.ValidDate = CAST(GETDATE() AS DATE)
              AND p.IsActive = 1;");

        // Create vw_RecentFetchActivity
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_RecentFetchActivity', 'V') IS NOT NULL
                DROP VIEW dbo.vw_RecentFetchActivity;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_RecentFetchActivity]
            AS
            SELECT TOP 1000
                fl.Id,
                fl.ProviderId,
                p.Code AS ProviderCode,
                p.Name AS ProviderName,
                fl.FetchStarted,
                fl.FetchCompleted,
                fl.Status,
                fl.RatesImported,
                fl.RatesUpdated,
                fl.DurationMs,
                fl.ErrorMessage,
                u.Email AS RequestedByEmail
            FROM [dbo].[ExchangeRateFetchLog] fl
            INNER JOIN [dbo].[ExchangeRateProvider] p ON fl.ProviderId = p.Id
            LEFT JOIN [dbo].[User] u ON fl.RequestedBy = u.Id
            ORDER BY fl.FetchStarted DESC;");

        // Create vw_ProviderHealthStatus (must be before vw_SystemHealthDashboard which depends on it)
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_ProviderHealthStatus', 'V') IS NOT NULL
                DROP VIEW dbo.vw_ProviderHealthStatus;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_ProviderHealthStatus]
            AS
            SELECT
                p.Id,
                p.Code,
                p.Name,
                p.IsActive,
                p.BaseCurrencyId,
                bc.Code AS BaseCurrencyCode,
                p.RequiresAuthentication,
                p.LastSuccessfulFetch,
                p.LastFailedFetch,
                p.ConsecutiveFailures,
                CASE
                    WHEN p.LastSuccessfulFetch IS NOT NULL
                    THEN DATEDIFF(HOUR, p.LastSuccessfulFetch, GETDATE())
                    ELSE NULL
                END AS HoursSinceLastSuccess,
                (SELECT COUNT(*)
                 FROM [dbo].[ExchangeRateFetchLog] fl
                 WHERE fl.ProviderId = p.Id
                   AND fl.FetchStarted >= DATEADD(DAY, -30, GETDATE())) AS TotalFetches30Days,
                (SELECT COUNT(*)
                 FROM [dbo].[ExchangeRateFetchLog] fl
                 WHERE fl.ProviderId = p.Id
                   AND fl.Status = 'Success'
                   AND fl.FetchStarted >= DATEADD(DAY, -30, GETDATE())) AS SuccessfulFetches30Days,
                (SELECT COUNT(*)
                 FROM [dbo].[ExchangeRateFetchLog] fl
                 WHERE fl.ProviderId = p.Id
                   AND fl.Status = 'Failed'
                   AND fl.FetchStarted >= DATEADD(DAY, -30, GETDATE())) AS FailedFetches30Days,
                (SELECT AVG(CAST(fl.DurationMs AS BIGINT))
                 FROM [dbo].[ExchangeRateFetchLog] fl
                 WHERE fl.ProviderId = p.Id
                   AND fl.Status = 'Success'
                   AND fl.DurationMs IS NOT NULL
                   AND fl.FetchStarted >= DATEADD(DAY, -30, GETDATE())) AS AvgFetchDurationMs,
                CASE
                    WHEN p.IsActive = 0 THEN 'Disabled'
                    WHEN p.ConsecutiveFailures >= 5 THEN 'Critical'
                    WHEN p.ConsecutiveFailures >= 3 THEN 'Degraded'
                    WHEN p.LastSuccessfulFetch IS NULL THEN 'Never Fetched'
                    WHEN DATEDIFF(HOUR, p.LastSuccessfulFetch, GETDATE()) > 24 THEN 'Stale'
                    WHEN DATEDIFF(HOUR, p.LastSuccessfulFetch, GETDATE()) > 2 THEN 'Warning'
                    ELSE 'Healthy'
                END AS HealthStatus
            FROM [dbo].[ExchangeRateProvider] p
            INNER JOIN [dbo].[Currency] bc ON p.BaseCurrencyId = bc.Id;");

        // Create vw_SystemHealthDashboard
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_SystemHealthDashboard', 'V') IS NOT NULL
                DROP VIEW dbo.vw_SystemHealthDashboard;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_SystemHealthDashboard]
            AS
            SELECT 'TotalProviders' AS Metric, CAST(COUNT(*) AS VARCHAR(50)) AS Value, 'Info' AS Status, NULL AS Details FROM [dbo].[ExchangeRateProvider]
            UNION ALL
            SELECT 'ActiveProviders', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRateProvider] WHERE IsActive = 1
            UNION ALL
            SELECT 'QuarantinedProviders', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[vw_ProviderHealthStatus] WHERE HealthStatus = 'Critical'
            UNION ALL
            SELECT 'TotalCurrencies', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[Currency]
            UNION ALL
            SELECT 'TotalExchangeRates', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRate]
            UNION ALL
            SELECT 'LatestRateDate', CAST(COALESCE(CAST(MAX(ValidDate) AS VARCHAR(50)), '') AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRate]
            UNION ALL
            SELECT 'OldestRateDate', CAST(COALESCE(CAST(MIN(ValidDate) AS VARCHAR(50)), '') AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRate]
            UNION ALL
            SELECT 'TotalFetchesToday', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRateFetchLog] WHERE CAST(FetchStarted AS DATE) = CAST(GETDATE() AS DATE)
            UNION ALL
            SELECT 'SuccessfulFetchesToday', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRateFetchLog] WHERE CAST(FetchStarted AS DATE) = CAST(GETDATE() AS DATE) AND Status = 'Success'
            UNION ALL
            SELECT 'FailedFetchesToday', CAST(COUNT(*) AS VARCHAR(50)), 'Info', NULL FROM [dbo].[ExchangeRateFetchLog] WHERE CAST(FetchStarted AS DATE) = CAST(GETDATE() AS DATE) AND Status = 'Failed'
            UNION ALL
            SELECT 'SuccessRateToday',
                CAST(
                    CASE
                        WHEN COUNT(*) = 0 THEN 0
                        ELSE (100.0 * SUM(CASE WHEN Status = 'Success' THEN 1 ELSE 0 END) / COUNT(*))
                    END AS VARCHAR(50)
                ),
                'Info',
                NULL
            FROM [dbo].[ExchangeRateFetchLog]
            WHERE CAST(FetchStarted AS DATE) = CAST(GETDATE() AS DATE);");

        // Create vw_ErrorSummary
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.vw_ErrorSummary', 'V') IS NOT NULL
                DROP VIEW dbo.vw_ErrorSummary;");

        await connection.ExecuteAsync(@"
            CREATE VIEW [dbo].[vw_ErrorSummary]
            AS
            SELECT
                el.Id,
                el.Timestamp,
                el.Severity,
                el.Source,
                el.Message,
                u.Email AS UserEmail,
                DATEDIFF(MINUTE, el.Timestamp, GETDATE()) AS MinutesAgo
            FROM [dbo].[ErrorLog] el
            LEFT JOIN [dbo].[User] u ON el.UserId = u.Id
            WHERE el.Timestamp >= DATEADD(DAY, -30, GETDATE());");

        // Create stored procedure sp_BulkUpsertExchangeRates
        await EnsureStoredProceduresCreatedAsync(connection);
    }

    /// <summary>
    /// Creates stored procedures that are not managed by Entity Framework.
    /// </summary>
    private async Task EnsureStoredProceduresCreatedAsync(SqlConnection connection)
    {
        // Drop and recreate sp_BulkUpsertExchangeRates
        await connection.ExecuteAsync(@"
            IF OBJECT_ID('dbo.sp_BulkUpsertExchangeRates', 'P') IS NOT NULL
                DROP PROCEDURE dbo.sp_BulkUpsertExchangeRates;");

        await connection.ExecuteAsync(@"
            CREATE PROCEDURE [dbo].[sp_BulkUpsertExchangeRates]
                @ProviderId INT,
                @ValidDate DATE,
                @RatesJson NVARCHAR(MAX)
            AS
            BEGIN
                SET NOCOUNT ON;
                SET XACT_ABORT ON;

                DECLARE @InsertedCount INT = 0;
                DECLARE @UpdatedCount INT = 0;
                DECLARE @SkippedCount INT = 0;
                DECLARE @BaseCurrencyId INT;

                BEGIN TRY
                    SELECT @BaseCurrencyId = BaseCurrencyId
                    FROM [dbo].[ExchangeRateProvider]
                    WHERE Id = @ProviderId;

                    IF @BaseCurrencyId IS NULL
                        THROW 50100, 'Provider not found or has no base currency configured', 1;

                    IF @RatesJson IS NULL OR @RatesJson = ''
                        THROW 50101, 'Rates JSON cannot be empty', 1;

                    IF ISJSON(@RatesJson) = 0
                        THROW 50102, 'Invalid JSON format', 1;

                    DECLARE @HistoricalDataDays INT;
                    DECLARE @CutoffDate DATE;

                    SELECT @HistoricalDataDays = TRY_CAST([Value] AS INT)
                    FROM [dbo].[SystemConfiguration]
                    WHERE [Key] = 'HistoricalDataDays';

                    IF @HistoricalDataDays IS NULL
                        SET @HistoricalDataDays = 90;

                    SET @CutoffDate = DATEADD(DAY, -@HistoricalDataDays, GETUTCDATE());

                    IF @ValidDate < @CutoffDate
                    BEGIN
                        SELECT
                            0 AS InsertedCount,
                            0 AS UpdatedCount,
                            (SELECT COUNT(*) FROM OPENJSON(@RatesJson)) AS SkippedCount,
                            0 AS ProcessedCount,
                            (SELECT COUNT(*) FROM OPENJSON(@RatesJson)) AS TotalInJson,
                            'SKIPPED - Date is outside ' + CAST(@HistoricalDataDays AS NVARCHAR(10)) + '-day range' AS [Status];
                        RETURN;
                    END

                    BEGIN TRANSACTION;

                    DECLARE @OriginalJsonCount INT = (SELECT COUNT(*) FROM OPENJSON(@RatesJson));

                    ;WITH ParsedRates AS (
                        SELECT
                            JSON_VALUE(value, '$.currencyCode') AS CurrencyCode,
                            TRY_CAST(JSON_VALUE(value, '$.rate') AS DECIMAL(19,6)) AS Rate,
                            TRY_CAST(JSON_VALUE(value, '$.multiplier') AS INT) AS Multiplier,
                            ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(value, '$.currencyCode') ORDER BY (SELECT NULL)) AS RowNum
                        FROM OPENJSON(@RatesJson)
                    )
                    SELECT
                        CurrencyCode,
                        Rate,
                        Multiplier
                    INTO #TempRates
                    FROM ParsedRates
                    WHERE RowNum = 1;

                    IF EXISTS (SELECT 1 FROM #TempRates WHERE CurrencyCode IS NULL OR Rate IS NULL)
                        THROW 50103, 'JSON contains invalid rate entries (missing currencyCode or rate)', 1;

                    UPDATE #TempRates SET Multiplier = 1 WHERE Multiplier IS NULL;

                    IF EXISTS (SELECT 1 FROM #TempRates WHERE Rate <= 0)
                        THROW 50104, 'All rates must be positive', 1;

                    IF EXISTS (SELECT 1 FROM #TempRates WHERE Multiplier <= 0)
                        THROW 50105, 'All multipliers must be positive', 1;

                    MERGE [dbo].[Currency] AS target
                    USING (
                        SELECT DISTINCT CurrencyCode
                        FROM #TempRates
                    ) AS source (Code)
                    ON target.Code = source.Code
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (Code)
                        VALUES (source.Code);

                    DECLARE @MergeOutput TABLE (
                        Action NVARCHAR(10),
                        ExchangeRateId INT
                    );

                    MERGE [dbo].[ExchangeRate] AS target
                    USING (
                        SELECT
                            @ProviderId AS ProviderId,
                            @BaseCurrencyId AS BaseCurrencyId,
                            c.Id AS TargetCurrencyId,
                            tr.Rate,
                            tr.Multiplier,
                            @ValidDate AS ValidDate
                        FROM #TempRates tr
                        INNER JOIN [dbo].[Currency] c ON tr.CurrencyCode = c.Code
                        WHERE c.Id <> @BaseCurrencyId
                    ) AS source
                    ON target.ProviderId = source.ProviderId
                       AND target.BaseCurrencyId = source.BaseCurrencyId
                       AND target.TargetCurrencyId = source.TargetCurrencyId
                       AND target.ValidDate = source.ValidDate
                    WHEN MATCHED THEN
                        UPDATE SET
                            Rate = source.Rate,
                            Multiplier = source.Multiplier,
                            Modified = SYSDATETIMEOFFSET()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (ProviderId, BaseCurrencyId, TargetCurrencyId, Rate, Multiplier, ValidDate)
                        VALUES (source.ProviderId, source.BaseCurrencyId, source.TargetCurrencyId,
                                source.Rate, source.Multiplier, source.ValidDate)
                    OUTPUT $action, INSERTED.Id
                    INTO @MergeOutput;

                    SELECT @InsertedCount = COUNT(*) FROM @MergeOutput WHERE Action = 'INSERT';
                    SELECT @UpdatedCount = COUNT(*) FROM @MergeOutput WHERE Action = 'UPDATE';

                    SET @SkippedCount = @OriginalJsonCount - (@InsertedCount + @UpdatedCount);

                    DROP TABLE #TempRates;

                    COMMIT TRANSACTION;

                    SELECT
                        @InsertedCount AS InsertedCount,
                        @UpdatedCount AS UpdatedCount,
                        @SkippedCount AS SkippedCount,
                        @InsertedCount + @UpdatedCount AS ProcessedCount,
                        @OriginalJsonCount AS TotalInJson,
                        'SUCCESS' AS Status;

                END TRY
                BEGIN CATCH
                    IF @@TRANCOUNT > 0
                        ROLLBACK TRANSACTION;

                    IF OBJECT_ID('tempdb..#TempRates') IS NOT NULL
                        DROP TABLE #TempRates;

                    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
                    DECLARE @ErrorNumber INT = ERROR_NUMBER();
                    DECLARE @ErrorLine INT = ERROR_LINE();

                    DECLARE @FullError NVARCHAR(4000) =
                        CONCAT('Bulk upsert failed: ', @ErrorMessage,
                               ' (Error ', @ErrorNumber, ' at line ', @ErrorLine, ')');
                    THROW 50199, @FullError, 1;
                END CATCH
            END;");
    }

    /// <summary>
    /// Called after each test. Cleans up resources.
    /// </summary>
    public virtual async Task DisposeAsync()
    {
        Scope.Dispose();
        if (ServiceProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
