curl -H "Content-Type: application/json" 172.18.0.1:9070/deployments -d '{"uri": "http://172.14.0.11:9080"}'
duckdb orders.db
sqlite3 orders.db < init.sql



curl -X POST https://crm.lug.center/v1/importdonhang/import \
                    -H "Content-Type: application/json" \
                    -d '{
                  "apikey": "fdd431311ab493f70700498bdae0eb380f6865ebcd9ec094ad134c0f3a3ab55e",
                  "data": [
                    {
                      "master": {
                        "ngayCT": "2026-03-14",
                        "maCT": "BL",
                        "soCT": "0002",
                        "BoPhan": "BP002",
                        "maDonHang": "BL/AETP_DDL/234325345/0002",
                        "tenKhachHang": "Nguyen Van A",
                        "soDienThoai": "0912345678",
                        "tinhThanh": "",
                        "quanHuyen": "",
                        "phuongXa": "",
                        "diaChi": ""
                      },
                      "detail": [
                        {
                          "maHang": "AB1195_20_ROSEGOLD",
                          "tenHang": "VALY NHỰA 8 BÁNH HIỆU ABER",
                          "imei": "12345678902353532",
                          "soLuong": 1,
                          "doanhThu": 25000000
                        }
                      ]
                    }
                  ]
                }'

curl -X POST https://crm.lug.center/v1/importdonhang/import \
                    -H "Content-Type: application/json" \
                    -d '{
                  "apikey": "fdd431311ab493f70700498bdae0eb380f6865ebcd9ec094ad134c0f3a3ab55e",

                  "data": [
                    {
                      "master": {
                        "ngayCT": "2026-03-14",
                        "maCT": "BL",
                        "soCT": "0001",
                        "BoPhan": "BP001",
                        "maDonHang": "DH-2026-001",
                        "tenKhachHang": "Nguyen Van A",
                        "soDienThoai": "0912345678",
                        "tinhThanh": "",
                        "quanHuyen": "",
                        "phuongXa": "",
                        "diaChi": ""
                      },
                      "detail": [
                        {
                          "maHang": "DH118MT4_20_BLACK",
                          "tenHang": "VALY NHỰA 8 BÁNH HIỆU DKNY DH118MT4_20_BLACK",
                          "imei": "123456789012345",
                          "soLuong": 1,
                          "doanhThu": 25000000
                        }
                      ]
                    }
                  ]
                }'
