using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quanlygym.BAL;

namespace Quanlygym.DAL
{
    public class DALayer
    {
        private readonly Neo4jService _neo4jService;

        // Khởi tạo tất cả BAL classes 
        private readonly DBKhachhang _dbKhachhang;
        private readonly DBNhanvien _dbNhanvien;
        private readonly DBPersonaltrainer _dbPersonaltrainer;
        private readonly DBThanhvien _dbThanhvien;
        private readonly DBThucphambosung _dbThucphambosung;
        private readonly DBLoaiTPBS _dbLoaiTPBS;
        private readonly DBNhacungcap _dbNhacungcap;
        private readonly DBHoaDon _dbHoaDon;
        private readonly DBChitieu _dbChitieu;
        private readonly DBLop _dbLop;
        private readonly DBLoailop _dbLoailop;
        private readonly DBNhaphang _dbNhaphang;

        public DALayer(Neo4jService neo4jService)
        {
            _neo4jService = neo4jService;

            // Khởi tạo tất cả BAL instances 
            _dbKhachhang = new DBKhachhang(neo4jService);
            _dbNhanvien = new DBNhanvien(neo4jService);
            _dbPersonaltrainer = new DBPersonaltrainer(neo4jService);
            _dbThanhvien = new DBThanhvien(neo4jService);
            _dbThucphambosung = new DBThucphambosung(neo4jService);
            _dbLoaiTPBS = new DBLoaiTPBS(neo4jService);
            _dbNhacungcap = new DBNhacungcap(neo4jService);
            _dbHoaDon = new DBHoaDon(neo4jService);
            _dbChitieu = new DBChitieu(neo4jService);
            _dbLop = new DBLop(neo4jService);
            _dbLoailop = new DBLoailop(neo4jService);
            _dbNhaphang = new DBNhaphang(neo4jService);
        }

        // Properties để truy cập các BAL classes từ bên ngoài
        public DBKhachhang KhachHang => _dbKhachhang;
        public DBNhanvien NhanVien => _dbNhanvien;
        public DBPersonaltrainer PersonalTrainer => _dbPersonaltrainer;
        public DBThanhvien ThanhVien => _dbThanhvien;
        public DBThucphambosung ThucPhamBoSung => _dbThucphambosung;
        public DBLoaiTPBS LoaiTPBS => _dbLoaiTPBS;
        public DBNhacungcap NhaCungCap => _dbNhacungcap;
        public DBHoaDon HoaDon => _dbHoaDon;
        public DBChitieu ChiTieu => _dbChitieu;
        public DBLop Lop => _dbLop;
        public DBLoailop LoaiLop => _dbLoailop;
        public DBNhaphang NhapHang => _dbNhaphang;

        // THỐNG KÊ VÀ BÁO CÁO

        /// <summary>
        /// Lấy thống kê tổng quan về hệ thống
        /// </summary>
        public async Task<Dictionary<string, object>> GetThongKeTongQuan()
        {
            var query = @"
                // Đếm tổng số khách hàng
                MATCH (p:Person) WHERE p.type = 'Khách hàng' 
                WITH COUNT(p) AS totalKhachHang
                
                // Đếm tổng số thành viên
                MATCH (p:Person) WHERE p.type = 'Thành viên' 
                WITH totalKhachHang, COUNT(p) AS totalThanhVien
                
                // Đếm tổng số nhân viên
                MATCH (p:Person) WHERE p.type = 'Nhân viên' 
                WITH totalKhachHang, totalThanhVien, COUNT(p) AS totalNhanVien
                
                // Đếm tổng số PT
                MATCH (p:Person) WHERE p.type = 'PT' 
                WITH totalKhachHang, totalThanhVien, totalNhanVien, COUNT(p) AS totalPT
                
                // Đếm tổng số sản phẩm
                MATCH (pr:Product) 
                WITH totalKhachHang, totalThanhVien, totalNhanVien, totalPT, COUNT(pr) AS totalSanPham
                
                // Đếm tổng số lớp học
                MATCH (c:Class) 
                WITH totalKhachHang, totalThanhVien, totalNhanVien, totalPT, totalSanPham, COUNT(c) AS totalLopHoc
                
                // Tính tổng doanh thu từ hóa đơn
                MATCH (t:Transaction) WHERE t.type = 'Hóa đơn' AND t.status = 'Completed'
                WITH totalKhachHang, totalThanhVien, totalNhanVien, totalPT, totalSanPham, totalLopHoc, 
                     SUM(t.total_amount) AS totalDoanhThu
                
                // Tính tổng chi phí
                MATCH (t:Transaction) WHERE t.type = 'Chi tiêu' AND t.status = 'Completed'
                WITH totalKhachHang, totalThanhVien, totalNhanVien, totalPT, totalSanPham, totalLopHoc, totalDoanhThu,
                     SUM(t.total_amount) AS totalChiPhi
                
                RETURN totalKhachHang, totalThanhVien, totalNhanVien, totalPT, 
                       totalSanPham, totalLopHoc, totalDoanhThu, totalChiPhi";

            var result = await _neo4jService.ExecuteQueryAsync(query);
            var thongKe = new Dictionary<string, object>();

            if (result.Count > 0)
            {
                var record = result[0];
                thongKe["TotalKhachHang"] = record["totalKhachHang"].As<int>();
                thongKe["TotalThanhVien"] = record["totalThanhVien"].As<int>();
                thongKe["TotalNhanVien"] = record["totalNhanVien"].As<int>();
                thongKe["TotalPT"] = record["totalPT"].As<int>();
                thongKe["TotalSanPham"] = record["totalSanPham"].As<int>();
                thongKe["TotalLopHoc"] = record["totalLopHoc"].As<int>();
                thongKe["TotalDoanhThu"] = record["totalDoanhThu"].As<long>();
                thongKe["TotalChiPhi"] = record["totalChiPhi"].As<long>();
                thongKe["LoiNhuan"] = record["totalDoanhThu"].As<long>() - record["totalChiPhi"].As<long>();
            }

            return thongKe;
        }

        /// <summary>
        /// Lấy báo cáo tồn kho (sử dụng Warehouse node từ CSDL)
        /// </summary>
        public async Task<List<Dictionary<string, object>>> GetBaoCaoTonKho()
        {
            var query = @"
                MATCH (p:Product)-[s:STORED_IN]->(w:Warehouse)
                RETURN p.id AS ProductId, p.name AS ProductName, 
                       p.quantity AS Quantity, p.min_quantity AS MinQuantity,
                       p.status AS Status, w.name AS WarehouseName,
                       CASE 
                         WHEN p.quantity <= p.min_quantity THEN 'SẮP HẾT'
                         WHEN p.quantity = 0 THEN 'HẾT HÀNG'
                         ELSE 'ĐỦ'
                       END AS TrangThaiTonKho
                ORDER BY p.quantity ASC, ProductName";

            return await _neo4jService.ExecuteQueryAsync(query);
        }

        /// <summary>
        /// Lấy top sản phẩm bán chạy
        /// </summary>
        public async Task<List<Dictionary<string, object>>> GetTopSanPhamBanChay(int top = 10)
        {
            var query = @"
                MATCH (t:Transaction {type: 'Hóa đơn', status: 'Completed'})-[:INCLUDES]->(p:Product)
                RETURN p.id AS ProductId, p.name AS ProductName, 
                       SUM(t.quantity) AS TotalSold, 
                       SUM(t.total_amount) AS TotalRevenue,
                       p.popularity AS Popularity
                ORDER BY TotalSold DESC
                LIMIT $top";

            var parameters = new Dictionary<string, object> { { "top", top } };
            return await _neo4jService.ExecuteQueryAsync(query, parameters);
        }

        /// <summary>
        /// Lấy doanh thu theo tháng
        /// </summary>
        public async Task<List<Dictionary<string, object>>> GetDoanhThuTheoThang(int year)
        {
            var query = @"
                MATCH (t:Transaction) 
                WHERE t.type = 'Hóa đơn' AND t.status = 'Completed' AND date(t.transaction_time).year = $year
                RETURN date(t.transaction_time).month AS Thang, 
                       SUM(t.total_amount) AS DoanhThu,
                       COUNT(t) AS SoHoaDon
                ORDER BY Thang";

            var parameters = new Dictionary<string, object> { { "year", year } };
            return await _neo4jService.ExecuteQueryAsync(query, parameters);
        }

        // TÌM KIẾM TỔNG HỢP

        /// <summary>
        /// Tìm kiếm tổng hợp across multiple entities
        /// </summary>
        public async Task<Dictionary<string, List<Dictionary<string, object>>>> TimKiemTongHop(string keyword)
        {
            var result = new Dictionary<string, List<Dictionary<string, object>>>();

            var parameters = new Dictionary<string, object> { { "keyword", keyword } };

            // Tìm trong Person
            var personQuery = @"
                MATCH (p:Person)
                WHERE toLower(p.name) CONTAINS toLower($keyword) 
                   OR toLower(p.email) CONTAINS toLower($keyword)
                   OR toLower(p.phone) CONTAINS toLower($keyword)
                RETURN p.id AS Id, p.name AS Name, p.type AS Type, 
                       p.email AS Email, p.phone AS Phone
                LIMIT 20";

            result["Person"] = await _neo4jService.ExecuteQueryAsync(personQuery, parameters);

            // Tìm trong Product
            var productQuery = @"
                MATCH (p:Product)
                WHERE toLower(p.name) CONTAINS toLower($keyword) 
                   OR toLower(p.flavor) CONTAINS toLower($keyword)
                RETURN p.id AS Id, p.name AS Name, p.price AS Price, 
                       p.status AS Status, p.quantity AS Quantity
                LIMIT 20";

            result["Product"] = await _neo4jService.ExecuteQueryAsync(productQuery, parameters);

            return result;
        }

        // QUẢN LÝ QUAN HỆ

        /// <summary>
        /// Đăng ký khách hàng thành thành viên
        /// </summary>
        public async Task<bool> DangKyThanhVien(string khachHangId, string thanhVienId, DateTime startDate, DateTime endDate, string membershipType)
        {
            var query = @"
                MATCH (kh:Person {id: $khachHangId, type: 'Khách hàng'})
                MATCH (tv:Person {id: $thanhVienId, type: 'Thành viên'})
                CREATE (kh)-[:BECAME_MEMBER {
                    upgrade_date: $startDate,
                    membership_type: $membershipType,
                    start_date: $startDate,
                    end_date: $endDate
                }]->(tv)
                RETURN kh, tv";

            var parameters = new Dictionary<string, object>
            {
                { "khachHangId", khachHangId },
                { "thanhVienId", thanhVienId },
                { "startDate", startDate },
                { "endDate", endDate },
                { "membershipType", membershipType }
            };

            var result = await _neo4jService.ExecuteQueryAsync(query, parameters);
            return result.Count > 0;
        }

        /// <summary>
        /// Đăng ký thành viên vào lớp học
        /// </summary>
        public async Task<bool> DangKyLopHoc(string thanhVienId, string lopId, DateTime enrollmentDate, string status = "Active")
        {
            var query = @"
                MATCH (tv:Person {id: $thanhVienId, type: 'Thành viên'})
                MATCH (lop:Class {id: $lopId})
                CREATE (tv)-[:ENROLLED_IN {
                    enrollment_date: $enrollmentDate,
                    status: $status
                }]->(lop)
                RETURN tv, lop";

            var parameters = new Dictionary<string, object>
            {
                { "thanhVienId", thanhVienId },
                { "lopId", lopId },
                { "enrollmentDate", enrollmentDate },
                { "status", status }
            };

            var result = await _neo4jService.ExecuteQueryAsync(query, parameters);
            return result.Count > 0;
        }

        // UTILITY METHODS

        /// <summary>
        /// Kiểm tra kết nối database
        /// </summary>
        public async Task<bool> KiemTraKetNoi()
        {
            try
            {
                var query = "RETURN 1 AS ConnectionTest";
                var result = await _neo4jService.ExecuteQueryAsync(query);
                return result.Count > 0;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Lấy lịch sử hoạt động gần đây
        /// </summary>
        public async Task<List<Dictionary<string, object>>> GetLichSuHoatDong(int limit = 50)
        {
            var query = @"
                MATCH (t:Transaction)
                RETURN t.id AS Id, t.type AS Type, t.total_amount AS Amount,
                       t.transaction_time AS Time, 'Transaction' AS EntityType
                ORDER BY t.transaction_time DESC
                LIMIT $limit";

            var parameters = new Dictionary<string, object> { { "limit", limit } };
            return await _neo4jService.ExecuteQueryAsync(query, parameters);
        }
    }
}