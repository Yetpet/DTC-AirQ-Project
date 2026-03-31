#!/usr/bin/env python3
"""
Air Quality DBT Pipeline Runner
Execute all DBT transformations for air quality analytics
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(e.stderr)
        return False

def main():
    """Main pipeline execution"""
    print(" Starting Air Quality DBT Pipeline...")

    # Get the script directory
    script_dir = Path(__file__).parent
    dbt_dir = script_dir / "air_quality_dbt"
    os.chdir(dbt_dir)

    # Commands to run
    commands = [
        ("uv run dbt debug", "Validating DBT configuration"),
        ("uv run dbt run --select staging", "Running staging models"),
        ("uv run dbt run --select intermediate", "Running intermediate models"),
        ("uv run dbt run --select marts", "Running mart models"),
        ("uv run dbt test", "Running data quality tests"),
        ("uv run dbt docs generate", "Generating documentation")
    ]

    # Execute all commands
    success_count = 0
    for command, description in commands:
        if run_command(command, description):
            success_count += 1
        else:
            print(f"⚠️  Pipeline stopped due to failure in: {description}")
            break

    # Summary
    print(f"\nPipeline Summary: {success_count}/{len(commands)} steps completed")

    if success_count == len(commands):
        print("🎉 Pipeline completed successfully!")
        print("View documentation: uv run dbt docs serve")
    else:
        print("  Pipeline completed with errors. Check logs above.")
        sys.exit(1)

if __name__ == "__main__":
    main()