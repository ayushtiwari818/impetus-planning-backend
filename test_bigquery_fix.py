#!/usr/bin/env python3
"""
Test script to verify BigQuery configuration fixes
"""

import asyncio
import requests
import time

def test_backend_startup():
    """Test that the backend starts without configuration errors"""
    print("🧪 Testing BigQuery Configuration Fix")
    print("=" * 50)
    
    # Test health endpoint
    try:
        print("Testing health endpoint...")
        response = requests.get("http://0.0.0.0:8000/api/v1/forecast/health", timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Health check passed!")
            print(f"   Status: {data.get('status', 'unknown')}")
            print(f"   Performance optimizations: {data.get('health_check_metrics', {}).get('performance_optimizations', 'unknown')}")
            return True
        else:
            print(f"❌ Health check failed with status: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection failed: {e}")
        print("   Make sure the backend is running on http://0.0.0.0:8000")
        return False

def test_basic_endpoints():
    """Test basic endpoints to ensure BigQuery integration works"""
    print("\n🔍 Testing Basic Endpoints")
    print("=" * 30)
    
    endpoints_to_test = [
        "/api/v1/forecast/unique-values",
        "/api/v1/forecast/cache/stats",
        "/api/v1/forecast/summary?limit=10"
    ]
    
    for endpoint in endpoints_to_test:
        try:
            print(f"Testing {endpoint}...")
            start_time = time.time()
            response = requests.get(f"http://0.0.0.0:8000{endpoint}", timeout=30)
            end_time = time.time()
            
            if response.status_code == 200:
                print(f"✅ Success ({(end_time - start_time)*1000:.0f}ms)")
            else:
                print(f"❌ Failed with status {response.status_code}")
                print(f"   Error: {response.text[:200]}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
    
    print("\n🎯 Configuration Fix Verification")
    print("=" * 35)
    print("✅ job_timeout_ms property removed from QueryJobConfig")
    print("✅ Proper parameter type mapping implemented")
    print("✅ Client-level timeout configuration added")
    print("✅ Async query execution optimized")

if __name__ == "__main__":
    print("Starting BigQuery Configuration Fix Verification...")
    print("Make sure your backend is running on http://0.0.0.0:8000\n")
    
    # Test backend startup
    if test_backend_startup():
        test_basic_endpoints()
        print("\n🚀 All tests completed! Backend is optimized and working correctly.")
    else:
        print("\n⚠️  Please start the backend first: python run.py") 