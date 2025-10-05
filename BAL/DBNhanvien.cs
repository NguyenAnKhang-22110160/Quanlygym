using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DBL;
using Neo4j.Driver;

namespace BAL
{
	public class DBNhanvien : IDisposable
	{
		private readonly Neo4jService _neo4jService;

		public DBNhanvien()
		{
			_neo4jService = new Neo4jService();
		}

		#region CRUD OPERATIONS
		public async Task<bool> ThemNhanVien(string maNV, string tenNV, string gioiTinh, string sdt, string email, int luong)
		{
			try
			{
				string query = @"
                    CREATE (nv:Person {
                        id: $maNV,
                        name: $tenNV,
                        gender: $gioiTinh,
                        phone: $sdt,
                        email: $email,
                        type: 'Nhân viên',
                        salary: $luong,
                        position: 'Lễ tân',
                        start_date: date(),
                        status: 'Active'
                    })";

				var parameters = new { maNV, tenNV, gioiTinh, sdt, email, luong };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thêm nhân viên: {ex.Message}");
			}
		}

		public async Task<bool> SuaNhanVien(string maNV, string tenNV, string gioiTinh, string sdt, string email, int luong, string chucVu)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {id: $maNV, type: 'Nhân viên'})
                    SET nv.name = $tenNV,
                        nv.gender = $gioiTinh,
                        nv.phone = $sdt,
                        nv.email = $email,
                        nv.salary = $luong,
                        nv.position = $chucVu";

				var parameters = new { maNV, tenNV, gioiTinh, sdt, email, luong, chucVu };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi sửa nhân viên: {ex.Message}");
			}
		}

		public async Task<bool> XoaNhanVien(string maNV)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {id: $maNV, type: 'Nhân viên'})
                    DETACH DELETE nv";

				var parameters = new { maNV };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi xóa nhân viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetTatCaNhanVien()
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})
                    RETURN nv.id as MaNV, nv.name as TenNV, nv.gender as GioiTinh,
                           nv.phone as SDT, nv.email as Email, nv.salary as Luong,
                           nv.position as ChucVu, nv.start_date as NgayVaoLam,
                           nv.status as TrangThai
                    ORDER BY nv.name";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy danh sách nhân viên: {ex.Message}");
			}
		}
		#endregion

		#region ADVANCED QUERIES
		public async Task<List<Dictionary<string, object>>> GetHieuSuatNhanVien()
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    WITH nv, 
                         count(hd) as SoHoaDon,
                         sum(hd.total_amount) as TongDoanhThu,
                         avg(hd.total_amount) as TrungBinhHD
                    RETURN nv.name as TenNV, nv.position as ChucVu,
                           SoHoaDon, TongDoanhThu, 
                           round(TrungBinhHD) as TrungBinhHoaDon,
                           nv.salary as Luong
                    ORDER BY TongDoanhThu DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi phân tích hiệu suất nhân viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetNhanVienTheoChucVu()
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})
                    RETURN nv.position as ChucVu,
                           count(nv) as SoNhanVien,
                           avg(nv.salary) as LuongTrungBinh,
                           min(nv.salary) as LuongThapNhat,
                           max(nv.salary) as LuongCaoNhat
                    ORDER BY SoNhanVien DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy nhân viên theo chức vụ: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetLichSuHoaDonCuaNhanVien(string maNV)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {id: $maNV})-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    OPTIONAL MATCH (kh:Person)-[:PURCHASED]->(hd)
                    RETURN hd.id as MaHD, hd.transaction_time as ThoiGian,
                           hd.total_amount as TongTien, hd.quantity as SoLuong,
                           kh.name as KhachHang, hd.note as GhiChu
                    ORDER BY hd.transaction_time DESC
                    LIMIT 20";

				var parameters = new { maNV };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy lịch sử hóa đơn: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetTopNhanVienBanHang(int top = 5)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    WITH nv, 
                         count(hd) as SoHoaDon,
                         sum(hd.total_amount) as TongDoanhThu
                    RETURN nv.name as TenNV, nv.position as ChucVu,
                           SoHoaDon, TongDoanhThu,
                           nv.salary as Luong,
                           round(TongDoanhThu * 0.01) as DuKienThuong
                    ORDER BY TongDoanhThu DESC
                    LIMIT $top";

				var parameters = new { top };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy top nhân viên bán hàng: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetDoanhThuTheoNhanVienTheoThang(int thang, int nam)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    WHERE hd.transaction_time.month = $thang AND hd.transaction_time.year = $nam
                    WITH nv, 
                         sum(hd.total_amount) as DoanhThuThang,
                         count(hd) as SoHoaDonThang
                    RETURN nv.name as TenNV, nv.position as ChucVu,
                           DoanhThuThang, SoHoaDonThang,
                           round(DoanhThuThang / SoHoaDonThang) as TrungBinhHD
                    ORDER BY DoanhThuThang DESC";

				var parameters = new { thang, nam };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy doanh thu theo nhân viên: {ex.Message}");
			}
		}

		public async Task<Dictionary<string, object>> GetThongTinChiTietNhanVien(string maNV)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {id: $maNV, type: 'Nhân viên'})
                    OPTIONAL MATCH (nv)-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    OPTIONAL MATCH (nv)-[:MANAGES]->(ql:Person)
                    WITH nv, 
                         count(hd) as TongHoaDon,
                         sum(hd.total_amount) as TongDoanhThu,
                         count(ql) as SoNhanVienQuanLy
                    RETURN nv.id as MaNV, nv.name as TenNV, nv.gender as GioiTinh,
                           nv.phone as SDT, nv.email as Email, nv.salary as Luong,
                           nv.position as ChucVu, nv.start_date as NgayVaoLam,
                           nv.status as TrangThai, TongHoaDon, TongDoanhThu,
                           SoNhanVienQuanLy,
                           CASE 
                             WHEN TongHoaDon > 0 THEN round(TongDoanhThu / TongHoaDon)
                             ELSE 0 
                           END as TrungBinhHoaDon";

				var results = await _neo4jService.ExecuteQueryAsync(query, new { maNV });

				if (results.Count > 0)
				{
					var record = results[0];
					return new Dictionary<string, object>
					{
						["MaNV"] = record["MaNV"].As<string>(),
						["TenNV"] = record["TenNV"].As<string>(),
						["GioiTinh"] = record["GioiTinh"].As<string>(),
						["SDT"] = record["SDT"].As<string>(),
						["Email"] = record["Email"].As<string>(),
						["Luong"] = record["Luong"].As<int>(),
						["ChucVu"] = record["ChucVu"].As<string>(),
						["NgayVaoLam"] = record["NgayVaoLam"].As<LocalDate>().ToString(),
						["TrangThai"] = record["TrangThai"].As<string>(),
						["TongHoaDon"] = record["TongHoaDon"].As<int>(),
						["TongDoanhThu"] = record["TongDoanhThu"].As<long>(),
						["SoNhanVienQuanLy"] = record["SoNhanVienQuanLy"].As<int>(),
						["TrungBinhHoaDon"] = record["TrungBinhHoaDon"].As<long>()
					};
				}

				return null;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thông tin chi tiết nhân viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetNhanVienCoHieuSuatCao()
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    WITH nv, 
                         count(hd) as SoHoaDon,
                         sum(hd.total_amount) as TongDoanhThu,
                         avg(hd.total_amount) as TrungBinhHD
                    WHERE SoHoaDon >= 10 AND TongDoanhThu >= 10000000
                    RETURN nv.name as TenNV, nv.position as ChucVu,
                           SoHoaDon, TongDoanhThu,
                           round(TrungBinhHD) as TrungBinhHoaDon,
                           nv.salary as Luong,
                           round(TongDoanhThu * 0.015) as ThuongDuKien
                    ORDER BY TongDoanhThu DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy nhân viên có hiệu suất cao: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThongKeHoaDonTheoGio()
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {type: 'Nhân viên'})-[:PROCESSED]->(hd:Transaction {type: 'Hóa đơn'})
                    RETURN hd.transaction_time.hour as GioTrongNgay,
                           count(hd) as SoHoaDon,
                           avg(hd.total_amount) as TrungBinhGiaTri,
                           sum(hd.total_amount) as TongDoanhThu
                    ORDER BY GioTrongNgay";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thống kê hóa đơn theo giờ: {ex.Message}");
			}
		}

		public async Task<bool> CapNhatLuongNhanVien(string maNV, int luongMoi)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {id: $maNV, type: 'Nhân viên'})
                    SET nv.salary = $luongMoi";

				var parameters = new { maNV, luongMoi };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi cập nhật lương nhân viên: {ex.Message}");
			}
		}

		public async Task<bool> CapNhatChucVuNhanVien(string maNV, string chucVuMoi)
		{
			try
			{
				string query = @"
                    MATCH (nv:Person {id: $maNV, type: 'Nhân viên'})
                    SET nv.position = $chucVuMoi";

				var parameters = new { maNV, chucVuMoi };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi cập nhật chức vụ nhân viên: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetNhanVienQuanLy()
		{
			try
			{
				string query = @"
                    MATCH (ql:Person {type: 'Nhân viên'})-[:MANAGES]->(nv:Person)
                    WITH ql, count(nv) as SoNhanVienQuanLy
                    RETURN ql.name as TenQuanLy, ql.position as ChucVu,
                           SoNhanVienQuanLy, ql.salary as Luong
                    ORDER BY SoNhanVienQuanLy DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy nhân viên quản lý: {ex.Message}");
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