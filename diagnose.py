# diagnose.py
import os
import sys
from pathlib import Path

print("🔍 Running Diagnostic Check...")
print(f"Python version: {sys.version}")
print(f"Current working directory: {os.getcwd()}")

# Check if src package exists and is importable
try:
    from src.extract import RedditExtractor
    print("✅ src.extract import successful")
except ImportError as e:
    print(f"❌ src.extract import failed: {e}")

# Check if data directory exists
data_dir = Path('data')
print(f"Data directory exists: {data_dir.exists()}")

# Check file permissions
print(f"Write permission in current directory: {os.access('.', os.W_OK)}")

print("\n📋 Directory Structure:")
for root, dirs, files in os.walk('.'):
    level = root.replace('.', '').count(os.sep)
    indent = ' ' * 2 * level
    print(f"{indent}{os.path.basename(root)}/")
    subindent = ' ' * 2 * (level + 1)
    for file in files:
        if file.endswith('.py') or file.endswith('.ini'):
            print(f"{subindent}{file}")