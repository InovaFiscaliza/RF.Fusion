from rfpye.parser import parse_bin
import os, sys

# =================================================
# Path setup  (TEM que vir antes dos imports do projeto)
# =================================================
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CONFIG_PATH = "/RFFusion/src/appCataloga/server_volume/etc/appCataloga"
if CONFIG_PATH not in sys.path:
    sys.path.append(CONFIG_PATH)

# =================================================
# Imports do projeto (AGORA funciona)
# =================================================
from shared import errors
from stations import station_factory


# =================================================
# Test set
# =================================================
BINS = [
    "/mnt/reposfi/tmp/RFEye002093/p-333a28a4--rfeye002093_260104_T102140.bin",
    "/mnt/reposfi/tmp/RFEye002299/p-438fe846--rfeye_file_rfeye002299_150916_070537.bin",
    "/mnt/reposfi/tmp/RFEye002300/p-f6e75543--rfeye002223_Freq_Fixas_PEAK_200807_073903.bin",
    "/mnt/reposfi/tmp/RFEye002303/p-0eb18bfa--rfeye002303_250611_T222425.bin"
]

HOST = "rfeye002093"

# =================================================
# Stress loop
# =================================================
i = 0
n_bins = len(BINS)
discarded = 0

while True:
    bin_path = BINS[i % n_bins]

    try:
        bin_data_raw = parse_bin(bin_path)
        station = station_factory(
            bin_data=bin_data_raw,
            host_uid=HOST
        )
        _ = station.process()

    except errors.BinValidationError as e:
        # esperado em BINs inválidos
        discarded += 1

    except Exception as e:
        # isso sim é relevante para heap
        print(f"[UNEXPECTED ERROR] iter={i} bin={bin_path} err={e}")
        raise

    finally:
        bin_data_raw = None
        station = None

    i += 1
    if i % 50 == 0:
        print(f"processed={i} discarded={discarded}")
