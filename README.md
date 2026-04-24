# Lakebridge SQL Migration Framework

This repository contains scripts, notebooks, and outputs related to the analysis, profiling, and migration of SQL workloads toward Databricks using Lakebridge.

## Project Structure

- **.assistant/** — Internal assistant / config files  
- **.lakebridge/** — Lakebridge config (Databricks workspace)  
- **analyzer_results/** — Analyzer outputs (CustomerBackup)  
- **diverse_codes/** — Misc scripts & experiments  
- **doc/** — Documentation / roadmap  
- **switch_runs/** — Switch LLM transpiler outputs  
- **transpiler-bladebridge-output/** — Deterministic Bladebridge transpilation outputs  
- **.gitignore** — Ignored files (venv, cache, system files, local scripts)  
- **Lakebridge Switch Table Explorer.ipynb** — Notebook to analyze the output of the Switch LLM transpiler
- **master_notrbook**: First notebook of the workflow, Switch Transpiler call
- **notebook_runner_metadata**: Materialization of the tables by running those notebooks
- **README.md** — Documentation

Refer to the documentation for step-by-step instructions and detailed explanations of these tools.
