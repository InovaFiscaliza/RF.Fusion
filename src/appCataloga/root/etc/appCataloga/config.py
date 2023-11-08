#!/usr/bin/env python
"""	Constants used in the appCataloga scripts """

# output processing information
LOG_VERBOSE = True
LOG_TARGET = 'file' # 'file' or 'screen'
LOG_FILE = '/var/log/appCataloga_backup.log'

# appCataloga socket service configuration
SERVER_PORT = 5555
TOTAL_CONNECTIONS = 50
CATALOG_QUERY_TAG = "catalog"
BACKUP_QUERY_TAG = "backup"
START_TAG = "<json>"
END_TAG = "</json>"

# database configuration
SERVER_NAME = r'localhost'
RFM_DATABASE_NAME = 'RFDATA'
BKP_DATABASE_NAME = 'BPDATA'
DB_USER_NAME = 'appCataloga' 
DB_PASSWORD = '<app_pass>'

# backup module configuration
BACKUP_CONTROL_MODULE = "/root/RF.Fusion/src/appCataloga/root/usr/local/bin/appCataloga/backup_control.py"
BACKUP_SINGLE_HOST_MODULE = "/root/RF.Fusion/src/appCataloga/root/usr/local/bin/appCataloga/backup_single_host.py"
BKP_MAX_PROCESS = 10
BKP_TASK_EXECUTION_WAIT_TIME = 10
BKP_TASK_REQUEST_WAIT_TIME = 300
BKP_TASK_EXECUTION_TIMEOUT = 3600
BKP_HOST_ALLOTED_TIME_FRACTION = 0.8

# file processing module configuration
PROCESSING_CONTROL_MODULE = "/root/RF.Fusion/src/appCataloga/root/usr/local/bin/appCataloga/processing_control.py"


# general configuration
SECONDS_IN_MINUTE = 60
PYTHON_ENV = ['conda', 'activate', 'myenv']

# daemon standard due for backup file
DAEMON_CFG_FILE="/etc/node/indexerD.cfg"
TARGET_FOLDER="/mnt/repo/tmp"

# constants that control the script
# * constants used for folder search
# TODO: Change configuration parameters to load json or more standard config file

FOLDER_TO_WATCH = "/mnt/sfi-sensores-repo/Auto"
#FOLDER_TO_WORK = "D:/Google_Drive/Master/Project/TFM_Code/DoBox"
#FOLDER_TO_PLACE_RESULTS = "D:/Google_Drive/Master/Project/TFM_Code/OutBox"
#FOLDER_TO_ARCHIVE = "D:/Google_Drive/Master/Project/TFM_Code/DoneBox"
#FOLDER_TO_STORE_FAILED = "D:/Google_Drive/Master/Project/TFM_Code/ErrorBox"

#BIN_FILE = "D:/Google_Drive/Master/Project/TFM_Code/ReferenceData/SCAN_M_450470_rfeye002088_170426_162736.bin"

#control export methods
METADATA_TARGET_DB = False
METADATA_TARGET_FILE = True

# file processing parameters
FILE_EXTENSION_TO_WATCH = ".bin"
FILE_TO_PROCESS_REGEX = [r".*"]
TARGET_ROOT_PATH = "/mnt/sfi-sensores-repo/Processado"
TARGET_ROOT_URL = "http://sensorex02.anatel.gov.br"

CSV_OUTPUT_FOLDER = "/mnt/sfi-sensores-repo/Processado"
CSV_EXTENSION = ".csv"

CSV_COLUMN_ORDER = ['Description',
                    'Initial_Time',
                    'Final_Time',
                    'Sample_Duration',
                    'Num_Traces',
                    'Start_Frequency',
                    'Stop_Frequency',
                    'Trace_Type',
                    'RBW',
                    'Level_Units',
                    'Vector_Length',
                    'Script_Version',
                    'Equipment_ID',
                    'Latitude',
                    'Longitude',
                    'Altitude',
                    'Count_GPS',
                    'State',
                    'State_Code',
                    'County',
                    'District',
                    'File_Name',
                    'URL']

#TIME_TO_FINISH_FILE_TRANSFER = timedelta(seconds=60)
QUEUE_CHECK_PERIOD = 5

#Geocoding parameters
NOMINATIM_USER = '9272749a.anatel.gov.br@amer.teams.ms'

#Number of seconds to control file watcher delays
PERIOD_FOR_STOP_CHECK = 1
PERIOD_FOR_OLD_FILES_CHECK = 300

# Standard IS multipliers
KILO = 1000.0
MEGA = 1000000.0

# Default values for CRFS Bin File Translation/Processing
DEFAULT_VBW = 0.0
DEFAULT_DETECTOR = 'RMS'
DEFAULT_ATTENUATOR = 'N ull'
DEFAULT_AMPLIFIER = 'Null'
RBW_STEP_FACTOR = 2.0
MEASUREMENT_UNIT = {0:'dBm',
                    1:'dBμV/m',
                    '%':'dBm'}

DB_CRFS_BIN_EQUIPMENT_TYPE = 1
DB_CRFS_BIN_FILE_FILE = 1

#TODO: Set correct value for dBm unit 

MAXIMUM_GNSS_DEVIATION = 0.0005
MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS = 1000

#Dict to convert state names to codes based on nomintim service
STATE_CODES = {'Rondônia':'RO',
               'Acre':'AC',
               'Amazonas':'AM',
               'Roraima':'RR',
               'Pará':'PA',
               'Amapá':'AP',
               'Tocantins':'TO',
               'Maranhão':'MA',
               'Piauí':'PI',
               'Ceará':'CE',
               'Rio Grande do Norte':'RN',
               'Paraíba':'PB',
               'Pernambuco':'PE',
               'Alagoas':'AL',
               'Sergipe':'SE',
               'Bahia':'BA',
               'Minas Gerais':'MG',
               'Espírito Santo':'ES',
               'Rio de Janeiro':'RJ',
               'São Paulo':'SP',
               'Paraná':'PR',
               'Santa Catarina':'SC',
               'Rio Grande do Sul':'RS',
               'Mato Grosso do Sul':'MS',
               'Mato Grosso':'MT',
               'Goiás':'GO',
               'Distrito Federal':'DF'}

# Nomintim service parameters normalization
REQUIRED_ADDRESS_FIELD = {'State':['state'],
                          'County':['city','town'],
                          'District':['suburb']}
