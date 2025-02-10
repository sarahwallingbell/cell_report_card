@echo off
call C:\Users\sarah.wallingbell\AppData\Local\anaconda3\Scripts\activate.bat morph_utils_v3
python \\allen\programs\celltypes\workgroups\mousecelltypes\SarahWB\github_projects\cell_report_card\cell_report_card.py
call conda deactivate