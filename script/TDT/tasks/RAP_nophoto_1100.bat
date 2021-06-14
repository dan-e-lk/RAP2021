@echo off
REM This file will create a daily schedule task to download
REM RAP_nophoto at 11:00
REM You MUST run this batch file as an Administrator
SCHTASKS /CREATE /SC DAILY /TN TDT\RAP_nophoto /TR "C:\Users\kimdan\OneDrive - Government of Ontario\Desktop\dist\TDT.exe -u daniel.kim2@ontario.ca -p Timmins1! -j NER-RAP -l C:/DanielKimWork/_RAP/RawData -s QMQ0WI_2020-03-10 -t CSV -e All -x True -g False" /ST 11:00 /RU SYSTEM /f