#!/usr/bin/env python3
"""
Quick setup guide - Chạy với mock mode
Không cần Docker hay AWS account
"""

def main():
    print("="*60)
    print("EC2 Manager - Quick Setup")
    print("="*60)
    print("\nTo run with MOCK DATA (no AWS account needed):")
    print("  python run_mock_mode.py")
    print("\nThen open browser:")
    print("  http://localhost:8000?region=us-east-1")
    print("\n" + "="*60)
    print("\nMock mode includes:")
    print("  - 3 test instances")
    print("  - 2 test volumes")
    print("  - Cost optimization report")
    print("="*60)

if __name__ == '__main__':
    main()
