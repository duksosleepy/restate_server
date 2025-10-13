import asyncio
from datetime import datetime

import httpx

# API Configuration
API_URL = "https://crm.lug.center/v1/importdonhang/import"
API_KEY = "fdd431311ab493f70700498bdae0eb380f6865ebcd9ec094ad134c0f3a3ab55e"


def create_test_data():
    """Create test data for import"""
    return {
        "apikey": API_KEY,
        "data": [
            {
                "master": {
                    "ngayCT": "2025-10-07",
                    "maCT": "HG",
                    "soCT": "0147",
                    "maBoPhan": "KG_KOH_PMQ8",
                    "maDonHang": "HG/KG_KOH_PMQ8/20251007/0147",
                    "tenKhachHang": "CHI LY",
                    "soDienThoai": "0908140688",
                    "tinhThanh": "",
                    "quanHuyen": "",
                    "phuongXa": "",
                    "diaChi": "Q6",
                },
                "detail": [
                    {
                        "maHang": "BAOTRUMLUG_28",
                        "tenHang": "BAO TRÙM VALY LUG_28",
                        "imei": "",
                        "soLuong": 1,
                        "doanhThu": 0,
                    }
                ],
            }
        ],
    }


async def async_test():
    """Asynchronous test"""
    print("\n=== Asynchronous Test ===")

    headers = {"content-type": "application/json"}
    data = create_test_data()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(API_URL, headers=headers, json=data)

            print(f"Status Code: {response.status_code}")
            print(f"Headers: {dict(response.headers)}")
            print(f"Response: {response.text}")

            if response.status_code == 200:
                print("✅ Async import successful")
            else:
                print("❌ Async import failed")

    except httpx.TimeoutException:
        print("❌ Async request timed out")
    except httpx.RequestError as e:
        print(f"❌ Async request error: {e}")
    except Exception as e:
        print(f"❌ Async unexpected error: {e}")


def main():
    """Run tests"""
    print(f"Testing CRM Import API - {datetime.now()}")
    print(f"URL: {API_URL}")

    asyncio.run(async_test())


if __name__ == "__main__":
    main()
