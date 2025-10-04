using Neo4j.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BAL
{
	public class DBKhachhang : Neo4jBase
	{
		public DBKhachhang() : base() { }

		// Lấy tất cả khách hàng
		public async Task<List<Dictionary<string, object>>> LayKhachHang()
		{
			string cypher = @"
                MATCH (kh:KhachHang)
                RETURN kh.MaKH AS MaKH,
                       kh.TenKH AS TenKH,
                       kh.GioiTinh AS GioiTinh,
                       kh.SDT AS SDT,
                       kh.Email AS Email,
                       kh.DiaChi AS DiaChi,
                       kh.NgayTao AS NgayTao
                ORDER BY kh.MaKH";

			var records = await ExecuteReadQueryAsync(cypher);
			return ConvertToDictionaryList(records);
		}

		// Thêm khách hàng
		public async Task<bool> ThemKhachHang(string MaKH, string TenKH, string GioiTinh, int SDT, string Email, string DiaChi)
		{
			string cypher = @"
                CREATE (kh:KhachHang {
                    MaKH: $maKH,
                    TenKH: $tenKH,
                    GioiTinh: $gioiTinh,
                    SDT: $sdt,
                    Email: $email,
                    DiaChi: $diaChi,
                    NgayTao: datetime()
                })";

			var parameters = new
			{
				maKH = MaKH,
				tenKH = TenKH,
				gioiTinh = GioiTinh,
				sdt = SDT,
				email = Email,
				diaChi = DiaChi
			};

			return await ExecuteWriteQueryAsync(cypher, parameters);
		}

		// Sửa khách hàng
		public async Task<bool> SuaKhachHang(string MaKH, string TenKH, string GioiTinh, int SDT, string Email, string DiaChi)
		{
			string cypher = @"
                MATCH (kh:KhachHang {MaKH: $maKH})
                SET kh.TenKH = $tenKH,
                    kh.GioiTinh = $gioiTinh,
                    kh.SDT = $sdt,
                    kh.Email = $email,
                    kh.DiaChi = $diaChi,
                    kh.NgayCapNhat = datetime()";

			var parameters = new
			{
				maKH = MaKH,
				tenKH = TenKH,
				gioiTinh = GioiTinh,
				sdt = SDT,
				email = Email,
				diaChi = DiaChi
			};

			return await ExecuteWriteQueryAsync(cypher, parameters);
		}

		// Xóa khách hàng
		public async Task<bool> XoaKhachHang(string MaKH)
		{
			string cypher = @"
                MATCH (kh:KhachHang {MaKH: $maKH})
                DETACH DELETE kh";

			return await ExecuteWriteQueryAsync(cypher, new { maKH = MaKH });
		}

		// Tìm kiếm khách hàng
		public async Task<List<Dictionary<string, object>>> TimKiemKhachHang(string MaKH, string TenKH, string GioiTinh, int? SDT, string Email, string DiaChi)
		{
			var conditions = new List<string>();
			var parameters = new Dictionary<string, object>();

			if (!string.IsNullOrEmpty(MaKH))
			{
				conditions.Add("kh.MaKH = $maKH");
				parameters.Add("maKH", MaKH);
			}

			if (!string.IsNullOrEmpty(TenKH))
			{
				conditions.Add("kh.TenKH CONTAINS $tenKH");
				parameters.Add("tenKH", TenKH);
			}

			if (!string.IsNullOrEmpty(GioiTinh))
			{
				conditions.Add("kh.GioiTinh = $gioiTinh");
				parameters.Add("gioiTinh", GioiTinh);
			}

			if (SDT.HasValue)
			{
				conditions.Add("kh.SDT = $sdt");
				parameters.Add("sdt", SDT.Value);
			}

			if (!string.IsNullOrEmpty(Email))
			{
				conditions.Add("kh.Email CONTAINS $email");
				parameters.Add("email", Email);
			}

			if (!string.IsNullOrEmpty(DiaChi))
			{
				conditions.Add("kh.DiaChi CONTAINS $diaChi");
				parameters.Add("diaChi", DiaChi);
			}

			string whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

			string cypher = $@"
                MATCH (kh:KhachHang)
                {whereClause}
                RETURN kh.MaKH AS MaKH,
                       kh.TenKH AS TenKH,
                       kh.GioiTinh AS GioiTinh,
                       kh.SDT AS SDT,
                       kh.Email AS Email,
                       kh.DiaChi AS DiaChi";

			var records = await ExecuteReadQueryAsync(cypher, parameters);
			return ConvertToDictionaryList(records);
		}

		// Thống kê khách hàng theo giới tính
		public async Task<List<Dictionary<string, object>>> ThongKeKhachHangTheoGioiTinh()
		{
			string cypher = @"
                MATCH (kh:KhachHang)
                RETURN kh.GioiTinh AS GioiTinh,
                       COUNT(kh) AS SoLuong,
                       COLLECT(kh.MaKH) AS DanhSachMaKH
                ORDER BY SoLuong DESC";

			var records = await ExecuteReadQueryAsync(cypher);
			return ConvertToDictionaryList(records);
		}

		private List<Dictionary<string, object>> ConvertToDictionaryList(List<IRecord> records)
		{
			var result = new List<Dictionary<string, object>>();
			foreach (var record in records)
			{
				result.Add(ConvertToDictionary(record));
			}
			return result;
		}

		private Dictionary<string, object> ConvertToDictionary(IRecord record)
		{
			var dict = new Dictionary<string, object>();
			foreach (var key in record.Keys)
			{
				dict[key] = record[key];
			}
			return dict;
		}
	}
}