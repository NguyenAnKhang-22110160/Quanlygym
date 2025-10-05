using Neo4j.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DBL
{
    public class Neo4jService : IDisposable
    {
        private readonly IDriver _driver;
        private readonly string _databaseName;

        // Constructor với thông số kết nối CỤ THỂ của bạn
        public Neo4jService()
        {
            _driver = GraphDatabase.Driver(
                "bolt://127.0.0.1:7687",
                AuthTokens.Basic("neo4j", "12345678")
            );
            _databaseName = "gym"; // Database name của bạn
        }

        // Constructor với thông số tùy chỉnh (nếu cần)
        public Neo4jService(string uri, string username, string password, string databaseName = "gym")
        {
            _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(username, password));
            _databaseName = databaseName;
        }

        public async Task<List<IRecord>> ExecuteQueryAsync(string query, object parameters = null)
        {
            await using var session = _driver.AsyncSession(config => config.WithDatabase(_databaseName));
            return await session.ExecuteReadAsync(async tx =>
            {
                var result = await tx.RunAsync(query, parameters);
                return await result.ToListAsync();
            });
        }

        public async Task ExecuteWriteAsync(string query, object parameters = null)
        {
            await using var session = _driver.AsyncSession(config => config.WithDatabase(_databaseName));
            await session.ExecuteWriteAsync(async tx =>
            {
                await tx.RunAsync(query, parameters);
            });
        }

        public async Task<IRecord> ExecuteSingleAsync(string query, object parameters = null)
        {
            await using var session = _driver.AsyncSession(config => config.WithDatabase(_databaseName));
            return await session.ExecuteReadAsync(async tx =>
            {
                var result = await tx.RunAsync(query, parameters);
                return await result.SingleAsync();
            });
        }

        // Thêm method kiểm tra kết nối
        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                await using var session = _driver.AsyncSession(config => config.WithDatabase(_databaseName));
                var result = await session.RunAsync("RETURN 1 as test");
                var record = await result.SingleAsync();
                return record["test"].As<int>() == 1;
            }
            catch (Exception)
            {
                return false;
            }
        }

        // Thêm method lấy thông tin database
        public async Task<string> GetDatabaseInfoAsync()
        {
            try
            {
                var result = await ExecuteSingleAsync("CALL db.info()");
                return $"Database: {_databaseName}, Version: {result["version"]}";
            }
            catch (Exception ex)
            {
                return $"Lỗi lấy thông tin database: {ex.Message}";
            }
        }

        public void Dispose()
        {
            _driver?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}