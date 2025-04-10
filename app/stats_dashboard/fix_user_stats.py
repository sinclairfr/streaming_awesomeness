#!/usr/bin/env python3
"""
Script to fix corrupted user_stats.json with deeply nested 'users' structure
"""
import json
import os
import time
import sys
from pathlib import Path

def fix_user_stats_file(file_path, backup=True):
    """Fix a corrupted user_stats.json file with deeply nested users structure"""
    
    print(f"Fixing user stats file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found")
        return False
        
    # Create backup
    if backup:
        backup_file = f"{file_path}.bak.{int(time.time())}"
        try:
            import shutil
            shutil.copy2(file_path, backup_file)
            print(f"Created backup: {backup_file}")
        except Exception as e:
            print(f"Warning: Could not create backup: {e}")
    
    try:
        # Load corrupted file
        with open(file_path, 'r') as f:
            loaded_data = json.load(f)
        
        # Initialize fixed structure
        fixed_data = {
            "users": {},
            "last_updated": int(time.time())  # Use current time as fallback
        }
        
        # Copy last_updated if it exists
        if isinstance(loaded_data, dict) and "last_updated" in loaded_data:
            fixed_data["last_updated"] = loaded_data["last_updated"]
        
        # Extract user data from nested structure
        def extract_users(data, target_dict):
            """Recursively extract user data from nested structure"""
            if not isinstance(data, dict):
                return
                
            # Process this level's users
            for key, value in data.items():
                if key == "users" and isinstance(value, dict):
                    # If this is a 'users' dictionary, process its contents
                    for user_key, user_data in value.items():
                        if isinstance(user_data, dict) and "total_watch_time" in user_data:
                            # This looks like actual user data
                            target_dict[user_key] = user_data
                            print(f"Found user: {user_key}")
                        else:
                            # This might be another nested structure
                            extract_users({user_key: user_data}, target_dict)
                elif isinstance(value, dict):
                    # For any other dictionary, just recurse
                    extract_users({key: value}, target_dict)
        
        # Run extraction
        extract_users(loaded_data, fixed_data["users"])
        
        # Report results
        print(f"Extracted {len(fixed_data['users'])} users from corrupted structure")
        
        # Save fixed file
        with open(file_path, 'w') as f:
            json.dump(fixed_data, f, indent=2)
            
        print(f"Successfully saved fixed data to {file_path}")
        return True
            
    except Exception as e:
        print(f"Error fixing file: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Default path
    default_path = "/app/stats/user_stats.json"
    
    # Get path from command line argument if provided
    file_path = sys.argv[1] if len(sys.argv) > 1 else default_path
    
    # Fix the file
    success = fix_user_stats_file(file_path)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1) 