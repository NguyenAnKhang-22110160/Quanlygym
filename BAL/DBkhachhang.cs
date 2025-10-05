using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DBL;
using Neo4j.Driver;

namespace BAL
{
    public class DBKhachhang : IDisposable
    {
        private readonly Neo4jService _neo4jService;

        public DBKhachhang()
        {
            // Sử dụng kết nối mặc định đã được cấu hình trong Neo4jService
            _neo4jService = new Neo4jService();
        }

        #region CRUD OPERATIONS
        public async Task<bool> ThemKhachHang(string maKH, string tenKH, string gioiTinh, string sdt, string email, string diaChi)
        {
            try
            {
                string query = @"
                    CREATE (kh:Person {
                        id: $maKH, 
                        name: $tenKH, 
                        gender: $gioiTinh, 
                        phone: $sdt, 
                        email: $email, 
                        type: 'Khách hàng', 
                        address: $diaChi,
                        join_date: date(),
                        customer_type: 'Thường'
                    })";

                var parameters = new { maKH, tenKH, gioiTinh, sdt, email, diaChi };
                await _neo4jService.ExecuteWriteAsync(query, parameters);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi thêm khách hàng: {ex.Message}");
            }
        }

        public async Task<bool> SuaKhachHang(string maKH, string tenKH, string gioiTinh, string sdt, string email, string diaChi)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {id: $maKH, type: 'Khách hàng'})
                    SET kh.name = $tenKH,
                        kh.gender = $gioiTinh,
                        kh.phone = $sdt,
                        kh.email = $email,
                        kh.address = $diaChi";

                var parameters = new { maKH, tenKH, gioiTinh, sdt, email, diaChi };
                await _neo4jService.ExecuteWriteAsync(query, parameters);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi sửa khách hàng: {ex.Message}");
            }
        }

        public async Task<bool> XoaKhachHang(string maKH)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {id: $maKH, type: 'Khách hàng'})
                    DETACH DELETE kh";

                var parameters = new { maKH };
                await _neo4jService.ExecuteWriteAsync(query, parameters);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi xóa khách hàng: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetTatCaKhachHang()
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {type: 'Khách hàng'})
                    RETURN kh.id as MaKH, kh.name as TenKH, kh.gender as GioiTinh, 
                           kh.phone as SDT, kh.email as Email, kh.address as DiaChi,
                           kh.join_date as NgayThamGia, kh.customer_type as LoaiKH
                    ORDER BY kh.name";

                var results = await _neo4jService.ExecuteQueryAsync(query);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy danh sách khách hàng: {ex.Message}");
            }
        }
        #endregion

        #region ADVANCED QUERIES
        public async Task<List<Dictionary<string, object>>> TimKiemKhachHang(string tenKH = null, string diaChi = null, string gioiTinh = null)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {type: 'Khách hàng'})
                    WHERE ($tenKH IS NULL OR kh.name CONTAINS $tenKH)
                    AND ($diaChi IS NULL OR kh.address CONTAINS $diaChi)
                    AND ($gioiTinh IS NULL OR kh.gender = $gioiTinh)
                    RETURN kh.id as MaKH, kh.name as TenKH, kh.gender as GioiTinh, 
                           kh.phone as SDT, kh.email as Email, kh.address as DiaChi
                    ORDER BY kh.name";

                var parameters = new { tenKH, diaChi, gioiTinh };
                var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi tìm kiếm khách hàng: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetKhachHangMuaNhieuNhat(int top = 10)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {type: 'Khách hàng'})-[:PURCHASED]->(t:Transaction)
                    RETURN kh.id as MaKH, kh.name as TenKH, 
                           count(t) as SoHoaDon, sum(t.total_amount) as TongChiTieu
                    ORDER BY TongChiTieu DESC
                    LIMIT $top";

                var parameters = new { top };
                var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy khách hàng mua nhiều nhất: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetKhachHangVIP()
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {type: 'Khách hàng'})-[:PURCHASED]->(t:Transaction)
                    WITH kh, sum(t.total_amount) as TongChiTieu
                    WHERE TongChiTieu > 5000000
                    RETURN kh.id as MaKH, kh.name as TenKH, TongChiTieu,
                           kh.customer_type as LoaiKH
                    ORDER BY TongChiTieu DESC";

                var results = await _neo4jService.ExecuteQueryAsync(query);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy khách hàng VIP: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetLichSuMuaHang(string maKH)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {id: $maKH})-[:PURCHASED]->(t:Transaction)-[:INCLUDES]->(p:Product)
                    RETURN t.id as MaHD, t.transaction_time as ThoiGian,
                           p.name as SanPham, t.quantity as SoLuong,
                           t.total_amount as ThanhTien
                    ORDER BY t.transaction_time DESC";

                var parameters = new { maKH };
                var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy lịch sử mua hàng: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetGoiYSanPham(string maKH)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {id: $maKH})-[:PURCHASED]->(:Transaction)-[:INCLUDES]->(spDaMua:Product)
                    MATCH (spDaMua)-[:BELONGS_TO]->(danhMuc:Category)
                    MATCH (danhMuc)<-[:BELONGS_TO]-(spGoiY:Product)
                    WHERE NOT EXISTS {
                        MATCH (kh)-[:PURCHASED]->(:Transaction)-[:INCLUDES]->(spGoiY)
                    }
                    RETURN DISTINCT spGoiY.name as SanPham, 
                           danhMuc.name as DanhMuc,
                           spGoiY.price as Gia,
                           spGoiY.popularity as DoPhoBien
                    ORDER BY spGoiY.popularity DESC
                    LIMIT 5";

                var parameters = new { maKH };
                var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi gợi ý sản phẩm: {ex.Message}");
            }
        }

        public async Task<Dictionary<string, object>> GetThongTinChiTiet(string maKH)
        {
            try
            {
                string query = @"
                    MATCH (kh:Person {id: $maKH, type: 'Khách hàng'})
                    OPTIONAL MATCH (kh)-[:PURCHASED]->(t:Transaction)-[:INCLUDES]->(p:Product)
                    OPTIONAL MATCH (kh)-[:BECAME_MEMBER]->(tv:Person)
                    WITH kh, 
                         count(DISTINCT t) as TongDonHang,
                         sum(t.total_amount) as TongChiTieu,
                         collect(DISTINCT p.name) as SanPhamDaMua
                    RETURN kh.id as MaKH, kh.name as TenKH, kh.gender as GioiTinh,
                           kh.phone as SDT, kh.email as Email, kh.address as DiaChi,
                           kh.join_date as NgayThamGia, kh.customer_type as LoaiKH,
                           TongDonHang, TongChiTieu, SanPhamDaMua";

                var results = await _neo4jService.ExecuteQueryAsync(query, new { maKH });

                if (results.Count > 0)
                {
                    var record = results[0];
                    return new Dictionary<string, object>
                    {
                        ["MaKH"] = record["MaKH"].As<string>(),
                        ["TenKH"] = record["TenKH"].As<string>(),
                        ["GioiTinh"] = record["GioiTinh"].As<string>(),
                        ["SDT"] = record["SDT"].As<string>(),
                        ["Email"] = record["Email"].As<string>(),
                        ["DiaChi"] = record["DiaChi"].As<string>(),
                        ["NgayThamGia"] = record["NgayThamGia"].As<LocalDate>().ToString(),
                        ["LoaiKH"] = record["LoaiKH"].As<string>(),
                        ["TongDonHang"] = record["TongDonHang"].As<int>(),
                        ["TongChiTieu"] = record["TongChiTieu"].As<long>(),
                        ["SanPhamDaMua"] = record["SanPhamDaMua"].As<List<string>>()
                    };
                }

                return null;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy thông tin chi tiết: {ex.Message}");
            }
        }
        #endregion

        #region CONNECTION TEST METHODS
        public async Task<bool> KiemTraKetNoi()
        {
            try
            {
                return await _neo4jService.TestConnectionAsync();
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi kiểm tra kết nối: {ex.Message}");
            }
        }

        public async Task<string> GetDatabaseInfo()
        {
            try
            {
                return await _neo4jService.GetDatabaseInfoAsync();
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy thông tin database: {ex.Message}");
            }
        }
        #endregion

        #region HELPER METHODS
        private List<Dictionary<string, object>> ConvertToDictionaryList(List<IRecord> records)
        {
            var result = new List<Dictionary<string, object>>();

            foreach (var record in records)
            {
                var dict = new Dictionary<string, object>();
                foreach (var key in record.Keys)
                {
                    dict[key] = record[key]?.As<object>();
                }
                result.Add(dict);
            }

            return result;
        }
        #endregion

        public void Dispose()
        {
            _neo4jService?.Dispose();
        }
    }
}