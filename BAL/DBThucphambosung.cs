using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DBL;
using Neo4j.Driver;

namespace BAL
{
	public class DBThucphambosung : IDisposable
	{
		private readonly Neo4jService _neo4jService;

		public DBThucphambosung()
		{
			_neo4jService = new Neo4jService();
		}

		#region CRUD OPERATIONS
		public async Task<bool> ThemThucPham(string maTP, string maLoaiTP, string tenTP, double khoiLuong,
										   string huongVi, int soLanDung, int giaTien, int soLuong)
		{
			try
			{
				string query = @"
                    CREATE (tp:Product {
                        id: $maTP,
                        maLoaiTP: $maLoaiTP,
                        name: $tenTP,
                        weight: $khoiLuong,
                        flavor: $huongVi,
                        usage_count: $soLanDung,
                        price: $giaTien,
                        cost_price: $giaTien * 0.8,
                        quantity: $soLuong,
                        min_quantity: 10,
                        status: 'Available',
                        popularity: 'Medium'
                    })";

				var parameters = new { maTP, maLoaiTP, tenTP, khoiLuong, huongVi, soLanDung, giaTien, soLuong };
				await _neo4jService.ExecuteWriteAsync(query, parameters);

				// Tạo relationship với Category
				string queryRelation = @"
                    MATCH (tp:Product {id: $maTP}), (dm:Category {id: $maLoaiTP})
                    CREATE (tp)-[:BELONGS_TO]->(dm)";

				await _neo4jService.ExecuteWriteAsync(queryRelation, new { maTP, maLoaiTP });

				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thêm thực phẩm: {ex.Message}");
			}
		}

		public async Task<bool> SuaThucPham(string maTP, string maLoaiTP, string tenTP, double khoiLuong,
										  string huongVi, int soLanDung, int giaTien, int soLuong)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product {id: $maTP})
                    SET tp.maLoaiTP = $maLoaiTP,
                        tp.name = $tenTP,
                        tp.weight = $khoiLuong,
                        tp.flavor = $huongVi,
                        tp.usage_count = $soLanDung,
                        tp.price = $giaTien,
                        tp.quantity = $soLuong,
                        tp.cost_price = $giaTien * 0.8,
                        tp.status = CASE 
                            WHEN $soLuong <= tp.min_quantity THEN 'Low Stock'
                            ELSE 'Available'
                        END";

				var parameters = new { maTP, maLoaiTP, tenTP, khoiLuong, huongVi, soLanDung, giaTien, soLuong };
				await _neo4jService.ExecuteWriteAsync(query, parameters);

				// Cập nhật relationship với Category
				string queryDeleteRelation = @"
                    MATCH (tp:Product {id: $maTP})-[r:BELONGS_TO]->()
                    DELETE r";

				string queryCreateRelation = @"
                    MATCH (tp:Product {id: $maTP}), (dm:Category {id: $maLoaiTP})
                    CREATE (tp)-[:BELONGS_TO]->(dm)";

				await _neo4jService.ExecuteWriteAsync(queryDeleteRelation, new { maTP });
				await _neo4jService.ExecuteWriteAsync(queryCreateRelation, new { maTP, maLoaiTP });

				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi sửa thực phẩm: {ex.Message}");
			}
		}

		public async Task<bool> XoaThucPham(string maTP)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product {id: $maTP})
                    DETACH DELETE tp";

				var parameters = new { maTP };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi xóa thực phẩm: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetTatCaThucPham()
		{
			try
			{
				string query = @"
                    MATCH (tp:Product)-[:BELONGS_TO]->(dm:Category)
                    RETURN tp.id as MaTP, tp.maLoaiTP as MaLoaiTP, tp.name as TenTP,
                           tp.weight as KhoiLuong, tp.flavor as HuongVi, 
                           tp.usage_count as SoLanDung, tp.price as GiaTien,
                           tp.quantity as SoLuong, tp.status as TrangThai,
                           tp.popularity as DoPhoBien, dm.name as TenDanhMuc
                    ORDER BY tp.name";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy danh sách thực phẩm: {ex.Message}");
			}
		}
		#endregion

		#region ADVANCED QUERIES
		public async Task<List<Dictionary<string, object>>> GetSanPhamBanChay(int top = 10)
		{
			try
			{
				string query = @"
                    MATCH (t:Transaction {type: 'Hóa đơn'})-[:INCLUDES]->(p:Product)
                    WITH p, sum(t.quantity) as TongSoLuongBan
                    RETURN p.name as TenSP, TongSoLuongBan as SoLuongBan, 
                           p.price as Gia, p.usage_count as SoLanDung,
                           p.popularity as DoPhobien,
                           (TongSoLuongBan * p.price) as DoanhThu
                    ORDER BY TongSoLuongBan DESC
                    LIMIT $top";

				var parameters = new { top };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy sản phẩm bán chạy: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetSanPhamSapHet()
		{
			try
			{
				string query = @"
                    MATCH (p:Product)
                    WHERE p.quantity <= p.min_quantity
                    RETURN p.id as MaTP, p.name as TenTP, p.quantity as SoLuong, 
                           p.min_quantity as SoLuongToiThieu, p.status as TrangThai,
                           p.price as Gia
                    ORDER BY p.quantity ASC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy sản phẩm sắp hết: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> PhanTichLoiNhuan()
		{
			try
			{
				string query = @"
                    MATCH (p:Product)
                    RETURN p.name as TenSP, p.price as GiaBan, p.cost_price as GiaVon,
                           (p.price - p.cost_price) as LoiNhuanDonVi,
                           p.usage_count as SoLanBan,
                           (p.price - p.cost_price) * p.usage_count as TongLoiNhuan,
                           round((p.price - p.cost_price) / p.cost_price * 100) as PhanTramLoiNhuan
                    ORDER BY TongLoiNhuan DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi phân tích lợi nhuận: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThucPhamTheoDanhMuc(string maLoaiTP)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product)-[:BELONGS_TO]->(dm:Category {id: $maLoaiTP})
                    RETURN tp.id as MaTP, tp.name as TenTP, tp.price as Gia,
                           tp.quantity as SoLuong, tp.status as TrangThai,
                           tp.popularity as DoPhoBien, tp.usage_count as SoLanDung
                    ORDER BY tp.usage_count DESC";

				var parameters = new { maLoaiTP };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thực phẩm theo danh mục: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> TimKiemThucPham(string tenTP = null, string huongVi = null,
																		  int? giaMin = null, int? giaMax = null)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product)
                    WHERE ($tenTP IS NULL OR tp.name CONTAINS $tenTP)
                    AND ($huongVi IS NULL OR tp.flavor CONTAINS $huongVi)
                    AND ($giaMin IS NULL OR tp.price >= $giaMin)
                    AND ($giaMax IS NULL OR tp.price <= $giaMax)
                    OPTIONAL MATCH (tp)-[:BELONGS_TO]->(dm:Category)
                    RETURN tp.id as MaTP, tp.name as TenTP, tp.flavor as HuongVi,
                           tp.price as Gia, tp.quantity as SoLuong, 
                           tp.status as TrangThai, dm.name as DanhMuc
                    ORDER BY tp.price DESC";

				var parameters = new { tenTP, huongVi, giaMin, giaMax };
				var results = await _neo4jService.ExecuteQueryAsync(query, parameters);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi tìm kiếm thực phẩm: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetDoanhThuTheoDanhMuc()
		{
			try
			{
				string query = @"
                    MATCH (t:Transaction {type: 'Hóa đơn'})-[:INCLUDES]->(tp:Product)-[:BELONGS_TO]->(dm:Category)
                    RETURN dm.name as DanhMuc, 
                           sum(t.total_amount) as DoanhThu,
                           count(t) as SoDonHang,
                           avg(t.total_amount) as TrungBinhDon,
                           sum(t.quantity) as TongSoLuongBan
                    ORDER BY DoanhThu DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thống kê doanh thu theo danh mục: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetThongKeTonKho()
		{
			try
			{
				string query = @"
                    MATCH (tp:Product)
                    WITH sum(tp.quantity * tp.cost_price) as TongGiaTriTonKho
                    MATCH (tp:Product)
                    RETURN tp.name as TenSP, tp.quantity as SoLuong,
                           tp.cost_price as GiaVon,
                           (tp.quantity * tp.cost_price) as GiaTriTonKho,
                           round((tp.quantity * tp.cost_price) / TongGiaTriTonKho * 100) as PhanTramTonKho
                    ORDER BY GiaTriTonKho DESC";

				var results = await _neo4jService.ExecuteQueryAsync(query);
				return ConvertToDictionaryList(results);
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi thống kê tồn kho: {ex.Message}");
			}
		}

		public async Task<Dictionary<string, object>> GetThongTinChiTietThucPham(string maTP)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product {id: $maTP})-[:BELONGS_TO]->(dm:Category)
                    OPTIONAL MATCH (t:Transaction {type: 'Hóa đơn'})-[:INCLUDES]->(tp)
                    WITH tp, dm, 
                         count(t) as SoDonHang,
                         sum(t.quantity) as TongSoLuongBan,
                         sum(t.total_amount) as TongDoanhThu
                    RETURN tp.id as MaTP, tp.name as TenTP, tp.maLoaiTP as MaLoaiTP,
                           tp.weight as KhoiLuong, tp.flavor as HuongVi,
                           tp.usage_count as SoLanDung, tp.price as GiaTien,
                           tp.cost_price as GiaVon, tp.quantity as SoLuong,
                           tp.min_quantity as SoLuongToiThieu, tp.status as TrangThai,
                           tp.popularity as DoPhoBien, dm.name as TenDanhMuc,
                           SoDonHang, TongSoLuongBan, TongDoanhThu,
                           (tp.price - tp.cost_price) as LoiNhuanDonVi,
                           ((tp.price - tp.cost_price) * TongSoLuongBan) as TongLoiNhuan";

				var results = await _neo4jService.ExecuteQueryAsync(query, new { maTP });

				if (results.Count > 0)
				{
					var record = results[0];
					return new Dictionary<string, object>
					{
						["MaTP"] = record["MaTP"].As<string>(),
						["TenTP"] = record["TenTP"].As<string>(),
						["MaLoaiTP"] = record["MaLoaiTP"].As<string>(),
						["KhoiLuong"] = record["KhoiLuong"].As<double>(),
						["HuongVi"] = record["HuongVi"].As<string>(),
						["SoLanDung"] = record["SoLanDung"].As<int>(),
						["GiaTien"] = record["GiaTien"].As<int>(),
						["GiaVon"] = record["GiaVon"].As<int>(),
						["SoLuong"] = record["SoLuong"].As<int>(),
						["SoLuongToiThieu"] = record["SoLuongToiThieu"].As<int>(),
						["TrangThai"] = record["TrangThai"].As<string>(),
						["DoPhoBien"] = record["DoPhoBien"].As<string>(),
						["TenDanhMuc"] = record["TenDanhMuc"].As<string>(),
						["SoDonHang"] = record["SoDonHang"].As<int>(),
						["TongSoLuongBan"] = record["TongSoLuongBan"].As<int>(),
						["TongDoanhThu"] = record["TongDoanhThu"].As<long>(),
						["LoiNhuanDonVi"] = record["LoiNhuanDonVi"].As<int>(),
						["TongLoiNhuan"] = record["TongLoiNhuan"].As<long>()
					};
				}

				return null;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi lấy thông tin chi tiết thực phẩm: {ex.Message}");
			}
		}

		public async Task<bool> CapNhatSoLuongThucPham(string maTP, int soLuongMoi)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product {id: $maTP})
                    SET tp.quantity = $soLuongMoi,
                        tp.status = CASE 
                            WHEN $soLuongMoi <= tp.min_quantity THEN 'Low Stock'
                            ELSE 'Available'
                        END";

				var parameters = new { maTP, soLuongMoi };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi cập nhật số lượng thực phẩm: {ex.Message}");
			}
		}

		public async Task<bool> TangSoLanDung(string maTP, int soLuong = 1)
		{
			try
			{
				string query = @"
                    MATCH (tp:Product {id: $maTP})
                    SET tp.usage_count = tp.usage_count + $soLuong,
                        tp.popularity = CASE 
                            WHEN tp.usage_count >= 50 THEN 'High'
                            WHEN tp.usage_count >= 20 THEN 'Medium'
                            ELSE 'Low'
                        END";

				var parameters = new { maTP, soLuong };
				await _neo4jService.ExecuteWriteAsync(query, parameters);
				return true;
			}
			catch (Exception ex)
			{
				throw new Exception($"Lỗi tăng số lần dùng: {ex.Message}");
			}
		}

		public async Task<List<Dictionary<string, object>>> GetSanPhamGoiY(string maKH)
		{
			try
			{
				string query = @"
                    // Tìm sản phẩm khách hàng đã mua
                    MATCH (kh:Person {id: $maKH})-[:PURCHASED]->(:Transaction)-[:INCLUDES]->(spDaMua:Product)
                    
                    // Tìm danh mục của sản phẩm đã mua
                    MATCH (spDaMua)-[:BELONGS_TO]->(danhMuc:Category)
                    
                    // Tìm sản phẩm cùng danh mục nhưng chưa mua
                    MATCH (danhMuc)<-[:BELONGS_TO]-(spGoiY:Product)
                    WHERE NOT EXISTS {
                        MATCH (kh)-[:PURCHASED]->(:Transaction)-[:INCLUDES]->(spGoiY)
                    }
                    
                    // Trả về sản phẩm gợi ý
                    RETURN DISTINCT spGoiY.name as SanPham, 
                           danhMuc.name as DanhMuc,
                           spGoiY.price as Gia,
                           spGoiY.popularity as DoPhoBien,
                           spGoiY.usage_count as SoLanBan
                    ORDER BY spGoiY.popularity DESC, spGoiY.usage_count DESC
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