using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DBL;
using Neo4j.Driver;

namespace BAL
{
	public class DBThanhvien : IDisposable
	{
		private readonly Neo4jService _neo4jService;

		public DBThanhvien()
		{
			_neo4jService = new Neo4jService();
		}

		#region CRUD OPERATIONS
		public async Task<bool> ThemThanhVien(string maTV, string tenTV, string gioiTinh, string sdt,
											string email, DateTime ngayBD, DateTime ngayKT)
		{
			try
			{
				string query = @"
                    CREATE (tv:Person {
                        id: $maTV,
                        name: $tenTV,
                        gender: $gioiTinh,
                        phone: $sdt,
                        email: $email,
                        type: 'Thành viên',
                        start_date: datetime($ngayBD),
                        end_date: datetime($ngayKT),
                        membership_type: '6 tháng',
                        status: 'Active'
                    })";

				var parameters = new { maTV, tenTV, gioiTinh, sdt, email, ngayBD, ngayKT };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thêm thành viên: {ex.Message}");
			}
		}

		public async Task<bool> SuaThanhVien(string maTV, string tenTV, string gioiTinh, string sdt,
										   string email, DateTime ngayBD, DateTime ngayKT, string trangThai)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV, type: 'Thành viên'})
                    SET tv.name = $tenTV,
                        tv.gender = $gioiTinh,
                        tv.phone = $sdt,
                        tv.email = $email,
                        tv.start_date = datetime($ngayBD),
                        tv.end_date = datetime($ngayKT),
                        tv.status = $trangThai";

				var parameters = new { maTV, tenTV, gioiTinh, sdt, email, ngayBD, ngayKT, trangThai };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi sửa thành viên: {ex.Message}");
			}
		}

		public async Task<bool> XoaThanhVien(string maTV)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV, type: 'Thành viên'})
                    DETACH DELETE tv";

				var parameters = new { maTV };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi xóa thành viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetTatCaThanhVien()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})
                    RETURN tv.id as MaTV, tv.name as TenTV, tv.gender as GioiTinh,
                           tv.phone as SDT, tv.email as Email, 
                           tv.start_date as NgayBatDau, tv.end_date as NgayKetThuc,
                           tv.membership_type as LoaiThanhVien, tv.status as TrangThai
                    ORDER BY tv.name";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy danh sách thành viên: {ex.Message}");
			}
		}
		#endregion

		#region ADVANCED QUERIES
		public async Task<List<Dictionary<string, object>>> GetThanhVienSapHetHan()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})
                    WHERE tv.end_date < datetime() + duration('P30D') AND tv.status = 'Active'
                    RETURN tv.name as TenTV, tv.end_date as NgayHetHan,
                           tv.membership_type as GoiTap, tv.status as TrangThai
                    ORDER BY tv.end_date ASC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thành viên sắp hết hạn: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThanhVienDaHetHan()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})
                    WHERE tv.end_date < datetime() AND tv.status = 'Active'
                    RETURN tv.name as TenTV, tv.end_date as NgayHetHan,
                           tv.membership_type as GoiTap
                    ORDER BY tv.end_date ASC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thành viên đã hết hạn: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetLichSuTapLuyen(string maTV)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV})-[:ATTENDED]->(session:TrainingSession)
                    OPTIONAL MATCH (pt:Person)-[:CONDUCTED]->(session)
                    RETURN session.date as NgayTap, session.duration as ThoiGian,
                           session.type as LoaiTap, session.calories_burned as Calories,
                           pt.name as HuongDanVien, session.intensity as CuongDo
                    ORDER BY session.date DESC";

				var parameters = new { maTV };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy lịch sử tập luyện: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetLopHocCuaThanhVien(string maTV)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV})-[:ENROLLED_IN]->(lop:Class)
                    OPTIONAL MATCH (pt:Person)-[:TEACHES]->(lop)
                    RETURN lop.id as MaLop, lop.start_date as NgayBatDau,
                           lop.end_date as NgayKetThuc, lop.cost as HocPhi,
                           pt.name as PT, lop.status as TrangThai
                    ORDER BY lop.start_date DESC";

				var parameters = new { maTV };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy lớp học của thành viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThanhVienTichCuc()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})-[:ATTENDED]->(session:TrainingSession)
                    WITH tv, count(session) as SoBuoiTap
                    WHERE SoBuoiTap >= 10
                    RETURN tv.name as TenTV, SoBuoiTap as SoBuoiThamGia,
                           tv.membership_type as GoiTap, tv.status as TrangThai
                    ORDER BY SoBuoiTap DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thành viên tích cực: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThongKeTapLuyenTheoThang()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})-[:ATTENDED]->(session:TrainingSession)
                    RETURN session.date.month as Thang,
                           count(session) as TongSoBuoiTap,
                           count(DISTINCT tv) as SoThanhVien,
                           avg(session.duration) as ThoiGianTrungBinh
                    ORDER BY Thang";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thống kê tập luyện: {ex.Message}");
			}
		}

		public async Task<Dictionary<string, object>> GetThongTinChiTietThanhVien(string maTV)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV, type: 'Thành viên'})
                    OPTIONAL MATCH (tv)-[:ENROLLED_IN]->(lop:Class)
                    OPTIONAL MATCH (tv)-[:ATTENDED]->(session:TrainingSession)
                    OPTIONAL MATCH (tv)-[:PURCHASED]->(hd:Transaction)
                    WITH tv, 
                         count(DISTINCT lop) as TongLopHoc,
                         count(DISTINCT session) as TongBuoiTap,
                         sum(hd.total_amount) as TongChiTieu
                    RETURN tv.id as MaTV, tv.name as TenTV, tv.gender as GioiTinh,
                           tv.phone as SDT, tv.email as Email, 
                           tv.start_date as NgayBatDau, tv.end_date as NgayKetThuc,
                           tv.membership_type as LoaiThanhVien, tv.status as TrangThai,
                           TongLopHoc, TongBuoiTap, TongChiTieu";

				var results = await _neo4jService.ExecuteQueryAsync(query, new { maTV });

				if (results.Count > 0)
				{
					var record = results[0];
					return new Dictionary<string, object>
					{
						["MaTV"] = record["MaTV"].As<string>(),
						["TenTV"] = record["TenTV"].As<string>(),
						["GioiTinh"] = record["GioiTinh"].As<string>(),
						["SDT"] = record["SDT"].As<string>(),
						["Email"] = record["Email"].As<string>(),
						["NgayBatDau"] = record["NgayBatDau"].As<ZonedDateTime>().ToDateTimeOffset().DateTime,
						["NgayKetThuc"] = record["NgayKetThuc"].As<ZonedDateTime>().ToDateTimeOffset().DateTime,
						["LoaiThanhVien"] = record["LoaiThanhVien"].As<string>(),
						["TrangThai"] = record["TrangThai"].As<string>(),
						["TongLopHoc"] = record["TongLopHoc"].As<int>(),
						["TongBuoiTap"] = record["TongBuoiTap"].As<int>(),
						["TongChiTieu"] = record["TongChiTieu"]?.As<long>() ?? 0
					};
				}

				return null;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thông tin chi tiết thành viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThanhVienTheoGoiTap()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})
                    RETURN tv.membership_type as GoiTap,
                           count(tv) as SoThanhVien,
                           avg(duration.between(tv.start_date, tv.end_date).days) as ThoiHanTrungBinh
                    ORDER BY SoThanhVien DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi phân tích theo gói tập: {ex.Message}");
			}
		}

		public async Task<bool> GiaHanThanhVien(string maTV, DateTime ngayKetThucMoi)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV, type: 'Thành viên'})
                    SET tv.end_date = datetime($ngayKetThucMoi),
                        tv.status = 'Active'";

				var parameters = new { maTV, ngayKetThucMoi };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi gia hạn thành viên: {ex.Message}");
			}
		}

		public async Task<bool> CapNhatTrangThaiThanhVien(string maTV, string trangThai)
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {id: $maTV, type: 'Thành viên'})
                    SET tv.status = $trangThai";

				var parameters = new { maTV, trangThai };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi cập nhật trạng thái thành viên: {ex.Message}");
			}
		}
		#endregion

		#region TRAINING ANALYSIS
		public async Task<List<Dictionary<string, object>>> GetThongKeLoaiHinhTap()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành viên'})-[:ATTENDED]->(session:TrainingSession)
                    RETURN session.type as LoaiHinhTap,
                           count(session) as SoBuoiTap,
                           avg(session.duration) as ThoiGianTrungBinh,
                           avg(session.calories_burned) as CaloriesTrungBinh
                    ORDER BY SoBuoiTap DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thống kê loại hình tập: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThanhVienCoHieuQuaTapLuyenCao()
		{
			try
			{
				string query = @"
                    MATCH (tv:Person {type: 'Thành vien'})-[:ATTENDED]->(session:TrainingSession)
                    WITH tv, 
                         count(session) as SoBuoiTap,
                         avg(session.calories_burned) as CaloriesTrungBinh,
                         avg(session.duration) as ThoiGianTrungBinh
                    WHERE SoBuoiTap >= 8 AND CaloriesTrungBinh >= 400
                    RETURN tv.name as TenTV, SoBuoiTap, 
                           round(CaloriesTrungBinh) as CaloriesTB,
                           round(ThoiGianTrungBinh) as ThoiGianTB
                    ORDER BY CaloriesTrungBinh DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thành viên có hiệu quả tập luyện cao: {ex.Message}");
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