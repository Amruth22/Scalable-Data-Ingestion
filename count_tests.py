#!/usr/bin/env python3
"""
Script to count and list the test cases
"""

import re

def count_tests():
    """Count test methods in tests.py"""
    with open('tests.py', 'r') as f:
        content = f.read()
    
    # Find all test methods
    test_methods = re.findall(r'def (test_\w+)\(', content)
    
    print("🧪 TEST CASE INVENTORY")
    print("=" * 50)
    print(f"Total test cases found: {len(test_methods)}")
    print()
    
    print("📋 Test Cases:")
    for i, test_name in enumerate(test_methods, 1):
        # Clean up test name for display
        display_name = test_name.replace('test_', '').replace('_', ' ').title()
        print(f"{i:2d}. {display_name}")
    
    print("=" * 50)
    
    if len(test_methods) == 10:
        print("✅ Perfect! Exactly 10 test cases as requested.")
    else:
        print(f"⚠️ Expected 10 test cases, found {len(test_methods)}")
    
    return test_methods

if __name__ == "__main__":
    count_tests()