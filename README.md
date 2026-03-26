# Lakebridge SQL Migration Framework

This repository contains scripts, notebooks, and outputs related to the analysis, profiling, and migration of SQL workloads toward Databricks using Lakebridge.

## Project Structure

.
├── .assistant/                         # Internal assistant / config files
├── .lakebridge/                        # Lakebridge configuration files (from databricks workspace)
├── analyzer_results/                   # Output from Lakebridge Analyzer on CustomerBackup 
├── diverse_codes/                      # Miscellaneous scripts and experiments 
├── doc/                                # Documentation: Roadmap to follow
├── switch_runs/                        # Output of the Switch LLM transpiler tool on CustomerBackup using various scripts / configurations 
├── transpiler-bladebridge-output/      # Output from SQL transpilation (Bladebridge) on CustomerBackup
├── .gitignore                          # Virtual environments (.venv/, venv/) / Cache files (__pycache__/) / System files (.DS_Store, Thumbs.db) / Local scripts and backups
├── Lakebridge Switch Table Explorer.ipynb  # Notebook for analyzing Switch outputs (from databricks worskpace)
├── README.md                           

Refer to the documentation for step-by-step instructions and detailed explanations of these tools.
