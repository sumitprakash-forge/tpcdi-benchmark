TPC-DI Data Generator (DIGen) — Setup
=====================================

This folder must contain the official TPC-DI Tools v1.1.0 data generator:

  - DIGen.jar
  - pdgf/   (directory; must be lowercase)

Download
--------

1. Go to: https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
2. Find "TPC-DI" (v1.1.0) under "Obsolete Workloads" and use the "Source Code" link, or:
   https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY
3. Download "TPC-DI_Tools_v1.1.0.zip" (or equivalent).
4. Unzip and copy into this folder (tools/datagen/):
   - DIGen.jar
   - The "PDGF" directory, RENAMED to lowercase: pdgf

Linux/Databricks: the generator expects "pdgf" (lowercase). The official ZIP may
use "PDGF"; rename it to "pdgf" after extracting.

Usage
-----

From the project root:

  python generate_tpcdi_data.py -s 10 -o dbfs:/mnt/tpcdi

Or from a Databricks notebook, call generate_tpcdi_data() from generate_tpcdi_data.py.

Specification: https://www.tpc.org/tpcdi/
