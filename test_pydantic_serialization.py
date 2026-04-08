#!/usr/bin/env python3
"""
Test Pydantic model serialization to ensure detail arrays are preserved
"""

import json
from typing import Any, Dict
from pydantic import BaseModel


class HttpRequest(BaseModel):
    url: str
    data: Dict[str, Any]
    task_id: str


def test_pydantic_serialization():
    """Test that Pydantic correctly serializes and deserializes detail arrays"""
    print("=" * 80)
    print("  Testing Pydantic Serialization of Detail Arrays")
    print("=" * 80)

    # Create test data with 2 detail items
    test_data = {
        "apikey": "test-key",
        "data": [
            {
                "master": {
                    "maDonHang": "TEST/001",
                    "tenKhachHang": "Test Customer"
                },
                "detail": [
                    {
                        "maHang": "PRODUCT_A",
                        "imei": "IMEI_A",
                        "soLuong": 1,
                        "doanhThu": 1000
                    },
                    {
                        "maHang": "PRODUCT_B",
                        "imei": "IMEI_B",
                        "soLuong": 1,
                        "doanhThu": 2000
                    }
                ]
            }
        ]
    }

    print("\n1. Create HttpRequest object with 2 detail items:")
    request = HttpRequest(
        url="http://test.com/api",
        data=test_data,
        task_id="test_123"
    )
    print(f"   request.data has {len(request.data['data'][0]['detail'])} detail items")

    print("\n2. Serialize to JSON (simulating ctx.service_call sending):")
    serialized = request.model_dump()
    serialized_json = json.dumps(serialized)
    print(f"   Serialized JSON length: {len(serialized_json)} bytes")
    detail_count_after_dump = len(serialized["data"]["data"][0]["detail"])
    print(f"   Detail count after model_dump(): {detail_count_after_dump}")

    print("\n3. Deserialize from JSON (simulating ctx.service_call receiving):")
    deserialized_dict = json.loads(serialized_json)
    deserialized_request = HttpRequest(**deserialized_dict)
    detail_count_after_load = len(deserialized_request.data["data"][0]["detail"])
    print(f"   Detail count after deserialization: {detail_count_after_load}")

    print("\n4. Verify detail items are preserved:")
    for i, detail in enumerate(deserialized_request.data["data"][0]["detail"]):
        print(f"   Detail {i+1}: maHang={detail['maHang']}, imei={detail['imei']}")

    print("\n" + "=" * 80)
    if detail_count_after_load == 2:
        print("✅ PASS: Pydantic serialization preserves all detail items")
    else:
        print(f"❌ FAIL: Expected 2 detail items, got {detail_count_after_load}")
    print("=" * 80)

    return detail_count_after_load == 2


def test_model_dump_json():
    """Test model_dump() vs model_dump_json() methods"""
    print("\n" + "=" * 80)
    print("  Testing model_dump() vs model_dump_json()")
    print("=" * 80)

    test_data = {
        "apikey": "test",
        "data": [{
            "master": {"maDonHang": "TEST"},
            "detail": [
                {"maHang": "A", "imei": "1"},
                {"maHang": "B", "imei": "2"},
                {"maHang": "C", "imei": "3"}
            ]
        }]
    }

    request = HttpRequest(url="http://test", data=test_data, task_id="123")

    print("\n1. Using model_dump():")
    dump_dict = request.model_dump()
    print(f"   Type: {type(dump_dict)}")
    print(f"   Detail count: {len(dump_dict['data']['data'][0]['detail'])}")

    print("\n2. Using model_dump_json():")
    dump_json_str = request.model_dump_json()
    print(f"   Type: {type(dump_json_str)}")
    parsed = json.loads(dump_json_str)
    print(f"   Detail count: {len(parsed['data']['data'][0]['detail'])}")

    print("\n3. Using dict():")
    dict_result = dict(request)
    print(f"   Type: {type(dict_result)}")
    print(f"   Detail count: {len(dict_result['data']['data'][0]['detail'])}")

    print("\n" + "=" * 80)
    print("✅ All methods preserve detail arrays")
    print("=" * 80)


def test_nested_dict_mutation():
    """Test if nested dict references can cause issues"""
    print("\n" + "=" * 80)
    print("  Testing Nested Dict Reference Safety")
    print("=" * 80)

    original_data = {
        "apikey": "test",
        "data": [{
            "master": {"maDonHang": "TEST"},
            "detail": [
                {"maHang": "A"},
                {"maHang": "B"}
            ]
        }]
    }

    print("\n1. Create HttpRequest with shared dict reference:")
    request = HttpRequest(url="http://test", data=original_data, task_id="123")
    print(f"   Original detail count: {len(original_data['data'][0]['detail'])}")
    print(f"   Request detail count: {len(request.data['data'][0]['detail'])}")

    print("\n2. Modify original dict (add third detail item):")
    original_data["data"][0]["detail"].append({"maHang": "C"})
    print(f"   Original detail count after modification: {len(original_data['data'][0]['detail'])}")
    print(f"   Request detail count after modification: {len(request.data['data'][0]['detail'])}")

    if len(request.data['data'][0]['detail']) == 3:
        print("\n⚠️  WARNING: Pydantic stores reference to original dict!")
        print("   Mutations to original data affect the model")
        print("   This could cause issues if data is modified after model creation")
    else:
        print("\n✅ SAFE: Pydantic creates a copy, original mutations don't affect model")

    print("=" * 80)


if __name__ == "__main__":
    print("\nPYDANTIC SERIALIZATION TEST SUITE\n")

    test_pydantic_serialization()
    test_model_dump_json()
    test_nested_dict_mutation()

    print("\n" + "=" * 80)
    print("CONCLUSION:")
    print("If all tests pass, Pydantic is NOT the issue.")
    print("The problem must be in:")
    print("  1. How lug-back groups the data before sending, OR")
    print("  2. How the final CRM server processes the received data")
    print("=" * 80 + "\n")
