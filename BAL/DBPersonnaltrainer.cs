using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DBL;
using Neo4j.Driver;

namespace BAL
{
    public class DBPersonaltrainer : IDisposable
    {
        private readonly Neo4jService _neo4jService;

        public DBPersonaltrainer()
        {
            _neo4jService = new Neo4jService();
        }

        #region CRUD OPERATIONS
        public async Task<bool> ThemPT(string maPT, string tenPT, string gioiTinh, string sdt, string email, int luong, int hoaHong)
        {
            try
            {
                string query = @"
                    CREATE (pt:Person {
                        id: $maPT,
                        name: $tenPT,
                        gender: $gioiTinh,
                        phone: $sdt,
                        email: $email,
                        type: 'PT',
                        salary: $luong,
                        commission: $hoaHong,
                        specialization: 'Fitness',
                        experience: 3,
                        start_date: date()
                    })";

                var parameters = new { maPT, tenPT, gioiTinh, sdt, email, luong, hoaHong };
                await _neo4jService.ExecuteWriteAsync(query, parameters);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi thêm PT: {ex.Message}");
            }
        }

        public async Task<bool> SuaPT(string maPT, string tenPT, string gioiTinh, string sdt, string email, int luong, int hoaHong)
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {id: $maPT, type: 'PT'})
                    SET pt.name = $tenPT,
                        pt.gender = $gioiTinh,
                        pt.phone = $sdt,
                        pt.email = $email,
                        pt.salary = $luong,
                        pt.commission = $hoaHong";

                var parameters = new { maPT, tenPT, gioiTinh, sdt, email, luong, hoaHong };
                await _neo4jService.ExecuteWriteAsync(query, parameters);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi sửa PT: {ex.Message}");
            }
        }

        public async Task<bool> XoaPT(string maPT)
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {id: $maPT, type: 'PT'})
                    DETACH DELETE pt";

                var parameters = new { maPT };
                await _neo4jService.ExecuteWriteAsync(query, parameters);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi xóa PT: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetTatCaPT()
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {type: 'PT'})
                    RETURN pt.id as MaPT, pt.name as TenPT, pt.gender as GioiTinh,
                           pt.phone as SDT, pt.email as Email, pt.salary as Luong,
                           pt.commission as HoaHong, pt.specialization as ChuyenMon,
                           pt.experience as KinhNghiem, pt.start_date as NgayVaoLam
                    ORDER BY pt.name";

                var results = await _neo4jService.ExecuteQueryAsync(query);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy danh sách PT: {ex.Message}");
            }
        }
        #endregion

        #region ADVANCED QUERIES
        public async Task<List<Dictionary<string, object>>> GetXepHangPT()
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {type: 'PT'})-[:TEACHES]->(lop:Class)
                    WITH pt, count(lop) as SoLop, sum(lop.cost) as TongDoanhThu
                    RETURN pt.name as TenPT, pt.salary as Luong, pt.commission as HoaHong,
                           SoLop, TongDoanhThu, (pt.salary + pt.commission) as ThuNhap
                    ORDER BY ThuNhap DESC";

                var results = await _neo4jService.ExecuteQueryAsync(query);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi xếp hạng PT: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetPTBanNhieuNhat()
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {type: 'PT'})-[:TEACHES]->(lop:Class)<-[:ENROLLED_IN]-(tv:Person)
                    WITH pt, count(DISTINCT tv) as SoHocVien
                    RETURN pt.name as TenPT, pt.specialization as ChuyenMon,
                           SoHocVien, pt.experience as KinhNghiem
                    ORDER BY SoHocVien DESC
                    LIMIT 10";

                var results = await _neo4jService.ExecuteQueryAsync(query);
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy PT bán nhiều nhất: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetLichDayCuaPT(string maPT)
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {id: $maPT})-[:TEACHES]->(lop:Class)
                    OPTIONAL MATCH (tv:Person)-[:ENROLLED_IN]->(lop)
                    RETURN lop.id as MaLop, lop.start_date as NgayBatDau,
                           lop.end_date as NgayKetThuc, lop.schedule as LichHoc,
                           lop.cost as HocPhi, count(tv) as SoHocVien,
                           lop.status as TrangThai
                    ORDER BY lop.start_date";

                var results = await _neo4jService.ExecuteQueryAsync(query, new { maPT });
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy lịch dạy của PT: {ex.Message}");
            }
        }

        public async Task<List<Dictionary<string, object>>> GetDanhGiaPT(string maPT)
        {
            try
            {
                string query = @"
                    MATCH (tv:Person)-[:WROTE]->(dg:Review)-[:ABOUT]->(pt:Person {id: $maPT})
                    RETURN tv.name as NguoiDanhGia, dg.rating as Diem,
                           dg.comment as NoiDung, dg.created_date as NgayDanhGia
                    ORDER BY dg.created_date DESC";

                var results = await _neo4jService.ExecuteQueryAsync(query, new { maPT });
                return ConvertToDictionaryList(results);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy đánh giá PT: {ex.Message}");
            }
        }

        public async Task<Dictionary<string, object>> GetThongTinChiTietPT(string maPT)
        {
            try
            {
                string query = @"
                    MATCH (pt:Person {id: $maPT, type: 'PT'})
                    OPTIONAL MATCH (pt)-[:TEACHES]->(lop:Class)
                    OPTIONAL MATCH (tv:Person)-[:ENROLLED_IN]->(lop)
                    OPTIONAL MATCH (tv)-[:WROTE]->(dg:Review)-[:ABOUT]->(pt)
                    WITH pt, 
                         count(DISTINCT lop) as TongLop,
                         count(DISTINCT tv) as TongHocVien,
                         collect(DISTINCT dg.rating) as DiemDanhGia
                    RETURN pt.name as TenPT, pt.gender as GioiTinh, pt.phone as SDT,
                           pt.email as Email, pt.salary as Luong, pt.commission as HoaHong,
                           pt.specialization as ChuyenMon, pt.experience as KinhNghiem,
                           TongLop, TongHocVien,
                           avg(DiemDanhGia) as DiemTrungBinh,
                           (pt.salary + pt.commission) as ThuNhap";

                var results = await _neo4jService.ExecuteQueryAsync(query, new { maPT });

                if (results.Count > 0)
                {
                    var record = results[0];
                    return new Dictionary<string, object>
                    {
                        ["TenPT"] = record["TenPT"].As<string>(),
                        ["GioiTinh"] = record["GioiTinh"].As<string>(),
                        ["SDT"] = record["SDT"].As<string>(),
                        ["Email"] = record["Email"].As<string>(),
                        ["Luong"] = record["Luong"].As<int>(),
                        ["HoaHong"] = record["HoaHong"].As<int>(),
                        ["ChuyenMon"] = record["ChuyenMon"].As<string>(),
                        ["KinhNghiem"] = record["KinhNghiem"].As<int>(),
                        ["TongLop"] = record["TongLop"].As<int>(),
                        ["TongHocVien"] = record["TongHocVien"].As<int>(),
                        ["DiemTrungBinh"] = record["DiemTrungBinh"]?.As<double>() ?? 0,
                        ["ThuNhap"] = record["ThuNhap"].As<int>()
                    };
                }

                return null;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi lấy thông tin chi tiết PT: {ex.Message}");
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