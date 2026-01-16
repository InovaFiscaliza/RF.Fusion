#!/usr/bin/python3
"""
Ultra-simple debug script.

Purpose:
- Parse multiple RFeye BIN files
- For each one, create:
    - bin_dataX      (RAW)
    - bin_dataX_rev  (REVISED)
- Keep EVERYTHING in memory
- Do NOT abstract
- Do NOT optimize
- Do NOT clean up

Designed for interactive debugging only.
"""

import sys
import os
import time
import traceback
import copy

# =================================================
# 1) Add PROJECT CODE ROOT (where stations/ exists)
# =================================================
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
# /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =================================================
# 2) Add CONFIG PATH (etc/appCataloga)
# =================================================
CONFIG_PATH = "/RFFusion/src/appCataloga/server_volume/etc/appCataloga"

if CONFIG_PATH not in sys.path:
    sys.path.append(CONFIG_PATH)

# =================================================
# IMPORTS
# =================================================
from rfpye.parser import parse_bin
from stations import station_factory
from pympler import asizeof
import shared as sh

print("Imports OK")

# =================================================
# FILES (explicit variables on purpose)
# =================================================
# Saida OK
filename0  = "/mnt/reposfi/2025/AL/2706703/8/rfeye002106_251210_T144800.bin"

#Processing Error | [ERROR] [stage=PROCESS] [type=AttributeError] Unexpected error while parsing BIN - 2038 linhas
# Aparentemente o enriquecimento aproveitou o arquivo incompleto
filename1  = "/mnt/reposfi/trash/rfeye_file_rfeye002307_160822_064053-RIO_05-PICO-700-950MHz.bin"

# Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Spectrum iterable is empty or invalid - 99 linhas
# Spectrum list vazio - descartado
filename2  = "/mnt/reposfi/tmp/RFEye002267/p-d6b614c3--rfeye002267_SMP700201215_045442.bin"

# Processing Error | [ERROR] [stage=PROCESS] [type=AttributeError] Processing failed 85 linhas
# GPS invalido - descartado
filename3  = "/mnt/reposfi/tmp/RFEye002182/p-2b7d9fa7--rfeye002182_SLMA_PEAK_200322_171202.bin"

# Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] spectrum[1]: invalid time window - 42 linhas
filename4  = "/mnt/reposfi/tmp/RFEye002267/p-f42cbee1--rfeye002267_SLMA_bimestral_occ15min_201215_045442.bin"

#Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] Missing required field: method - 13 linhas
filename5  = "/mnt/reposfi/tmp/RFEye002182/p-f869bbee--rfeye002182_TV_180427_110644.bin"

# Processing Error | [ERROR] [stage=PROCESS] [type=Exception] Processing failed - 13 linhas
# 'bw' faltando
# Ajustado por inferencia
filename6  = "/mnt/reposfi/tmp/RFEye002263/p-bb17209a--(none)_250116_T082500.bin"

# Processing Error | [ERROR] [stage=PROCESS] [type=AttributeError] PROCESS: Processing failed PROCESS: Processing failed - 5 linhas (momento do primeiro teste)
# GPS invalido
filename7  = "/mnt/reposfi/trash/rfeye002320-BRU_Freq_Fixas_OCC15min_181027_163531.bin"

# Processing Error | [ERROR] [stage=PROCESS] [type=AttributeError] PROCESS: Processing failed - 3 linhas (momento do segundo teste)
# GPS invalido
filename8  = "/mnt/reposfi/trash/rfeye002320-BRU_FM_OCC15min_181027_163531.bin"

# Missing dtype - Isso é um problema ?
# Corrigido dtype por inferencia
# spectrum[1] tem start_dateidx == stop_dateidx, logo foi descartado
filename9  = "/mnt/reposfi/tmp/RFEye002299/033940-SMA_espectros.bin"

# Faixa OCC para FM
# Ocupacao com nivel dbm
# GPS invalido
filename10 = "/mnt/reposfi/tmp/RFEye002300/rfeye002223_FM_occ15min_190410_112017.bin"

# Hostnames invertidos por troca de HD
filename11 = "/mnt/reposfi/tmp/RFEye002300/p-fb5f8813--rfeye_file_rfeye002223_180228_235555.bin"

# =================================================
# RAW PARSE
# =================================================

try:
    bin_data  = parse_bin(filename6)
    bin_data_antigo = copy.deepcopy(bin_data)
    # t0 = time.time()
    # size_mb = asizeof.asizeof(bin_data0) / 1024 / 1024
    # dt = time.time() - t0
    # print(f"[MEM] bin_data0 ocupa {size_mb:.2f} MB (calculado em {dt:.2f}s)")
    # bin_data1  = parse_bin(filename1)
    # bin_data2  = parse_bin(filename2)
    # bin_data3  = parse_bin(filename3)
    # bin_data4  = parse_bin(filename4)
    # bin_data5  = parse_bin(filename5)
    # bin_data6  = parse_bin(filename6)
    # bin_data7  = parse_bin(filename7)
    # bin_data8  = parse_bin(filename8)
    # bin_data9  = parse_bin(filename9)
    # bin_data10 = parse_bin(filename10)

    print("All parse_bin() calls completed")

except Exception as e:
    print("Error during parse_bin()")
    traceback.print_exc()

# =================================================
# STATION PROCESSING (one by one, explicit)
# =================================================

try:
    bin_data_rev  = station_factory(bin_data=bin_data,
                                     host_uid='rfeye002106').process()
except Exception as e:
    #bin_data0_rev = None
    print("bin_data_rev ERROR")
    traceback.print_exc()

print('Validei')
# try:
#     bin_data1_rev  = station_factory(bin_data1).process()
#     print("bin_data1_rev OK")
# except Exception as e:
#     bin_data1_rev = None
#     print("bin_data1_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data2_rev  = station_factory(bin_data2).process()
#     print("bin_data2_rev OK")
# except Exception as e:
#     bin_data2_rev = None
#     print("bin_data2_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data3_rev  = station_factory(bin_data3).process()
#     print("bin_data3_rev OK")
# except Exception as e:
#     bin_data3_rev = None
#     print("bin_data3_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data4_rev  = station_factory(bin_data4).process()
#     print("bin_data4_rev OK")
# except Exception as e:
#     bin_data4_rev = None
#     print("bin_data4_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data5_rev  = station_factory(bin_data5).process()
#     print("bin_data5_rev OK")
# except Exception as e:
#     bin_data5_rev = None
#     print("bin_data5_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data6_rev  = station_factory(bin_data6).process()
#     print("bin_data6_rev OK")
# except Exception as e:
#     bin_data6_rev = None
#     print("bin_data6_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data7_rev  = station_factory(bin_data7).process()
#     print("bin_data7_rev OK")
# except Exception as e:
#     bin_data7_rev = None
#     print("bin_data7_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data8_rev  = station_factory(bin_data8).process()
#     print("bin_data8_rev OK")
# except Exception as e:
#     bin_data8_rev = None
#     print("bin_data8_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data9_rev  = station_factory(bin_data9).process()
#     print("bin_data9_rev OK")
# except Exception as e:
#     bin_data9_rev = None
#     print("bin_data9_rev ERROR")
#     traceback.print_exc()

# try:
#     bin_data10_rev = station_factory(bin_data10).process()
#     print("bin_data10_rev OK")
# except Exception as e:
#     bin_data10_rev = None
#     print("bin_data10_rev ERROR")
#     traceback.print_exc()

print("\nScript finished. All variables are available for debugger inspection.")
