rfdata = {   
        'bridge_spectrun_emitter': {
            'id_bridge_emitter':0,
            'fk_emitter':0,
            'fk_spectrun':0
            },
        'bridge_spectrun_equipment': {
            'id_bridge_equipment':0,
            'fk_equipment':0,
            'fk_spectrun':0
            },
        'dim_equipment_type': {
            'id_equipment_type':0,
            'na_equipment_type':""
            },
        'dim_file_type': {
            'id_type_file':0,
            'na_type_file':""
            },
        'dim_measurement_procedure': {
            'id_procedure':0,
            'na_procedure':""
            },
        'dim_site_county': {
            'id_county_code':0,
            'fk_state_code':0,
            'na_county':""
            },
        'dim_site_district': {
            'id_district':0,
            'fk_county_code':0,
            'na_district':""
            },
        'dim_site_state': {
            'id_state_code':0,
            'na_state':"",
            'lc_state':""},
        'dim_spectrun_detector': {
            'id_detector_type':0,
            'na_detector_type':""},
        'dim_spectrun_emitter': {
            'id_emitter':0,
            'na_emitter':""},
        'dim_spectrun_equipment': {
            'id_equipment':0,
            'na_equipment':"",
            'fk_equipment_type':0},
        'dim_spectrun_file': {
            'id_file':0,
            'id_type_file':0,
            'na_file':"",
            'na_dir_e_file':"",
            'na_url':""},
        'dim_spectrun_site': {
            'id_site':0,
            'fk_site_district':0,
            'fk_county_code':0,
            'fk_state_code':0,
            'na_site':"",
            'geolocation':(0,0),
            'nu_altutude':0,
            'nu_gnss_measurements':0},
        'dim_spectrun_traco': {
            'id_trace_time':0,
            'na_trace_time':""},
        'dim_spectrun_unidade': {
            'id_measure_unit':0,
            'na_measure_unit':""},
        'fact_spectrun': {
            'id_fact_spectrun':0,
            'fk_file':0,
            'fk_site':0,
            'fk_detector_type':0,
            'fk_trace_time':0,
            'fk_measure_unit':0,
            'fk_procedure':0,
            'na_description':"",
            'nu_freq_start':0,
            'nu_freq_end':0,
            'dt_time_start':0,
            'dt_time_end':0,
            'nu_sample_duration':0,
            'nu_trace_count':0,
            'nu_trace_length':0,
            'nu_rbw':0,
            'nu_vbw':0,
            'nu_att_gain':0
            }
        }

bin_data = {
    'filename': 'rfeye002073_230331_T142000.bin',
    'file_version': 23,
    'string': 'CRFS DATA FILE V023',
    'hostname': 'RFeye002073',
    'method': 'Script2022_v2_Logger_Fixed.cfg',
    'unit_info': 'Fixed',
    'file_number': 0,
    'identifier': 'MESSAGE',
    'gps': '.latitude, .longitude, .altitude',
    'spectrum': [
        {type:67, 'thread_id':290, 'description':'PMEF 2022 (Faixa 10 de 10).', 'start_mega':3290, 'stop_mega':3799, 'dtype':'dBm', 'ndata':13056, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':310, 'description':'PMEC 2022 (Faixa 2 de 10).', 'start_mega':105, 'stop_mega':140, 'dtype':'dBm', 'ndata':3584, 'bw':18457, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':320, 'description':'PMEC 2022 (Faixa 3 de 10).', 'start_mega':155, 'stop_mega':165, 'dtype':'dBm', 'ndata':1024, 'bw':18457, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':340, 'description':'PMEC 2022 (Faixa 5 de 10).', 'start_mega':405, 'stop_mega':410, 'dtype':'dBm', 'ndata':512, 'bw':18457, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':100, 'description':'PRD 2022 (Faixa 1 de 4).', 'start_mega':50, 'stop_mega':90, 'dtype':'dBμV/m', 'ndata':1024, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':110, 'description':'PRD 2022 (Faixa 2 de 4).', 'start_mega':70, 'stop_mega':110, 'dtype':'dBμV/m', 'ndata':2048, 'bw':36914, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':120, 'description':'PRD 2022 (Faixa 3 de 4).', 'start_mega':170, 'stop_mega':220, 'dtype':'dBμV/m', 'ndata':1280, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':130, 'description':'PRD 2022 (Faixa 4 de 4).', 'start_mega':470, 'stop_mega':700, 'dtype':'dBμV/m', 'ndata':5888, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':300, 'description':'PMEC 2022 (Faixa 1 de 10).', 'start_mega':70, 'stop_mega':80, 'dtype':'dBm', 'ndata':1024, 'bw':18457, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':330, 'description':'PMEC 2022 (Faixa 4 de 10).', 'start_mega':325, 'stop_mega':340, 'dtype':'dBm', 'ndata':1536, 'bw':18457, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':350, 'description':'PMEC 2022 (Faixa 6 de 10).', 'start_mega':960, 'stop_mega':1429, 'dtype':'dBm', 'ndata':12032, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':360, 'description':'PMEC 2022 (Faixa 7 de 10).', 'start_mega':1530, 'stop_mega':1649, 'dtype':'dBm', 'ndata':3072, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':370, 'description':'PMEC 2022 (Faixa 8 de 10).', 'start_mega':2690, 'stop_mega':2899, 'dtype':'dBm', 'ndata':5376, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':380, 'description':'PMEC 2022 (Faixa 9 de 10).', 'start_mega':5000, 'stop_mega':5160, 'dtype':'dBm', 'ndata':4096, 'bw':73828, 'processing':'peak', 'antuid':0},
        {type:67, 'thread_id':390, 'description':'PMEC 2022 (Faixa 10 de 10).', 'start_mega':5339, 'stop_mega':5459, 'dtype':'dBm', 'ndata':3328, 'bw':73828, 'processing':'peak', 'antuid':0}
        ]
    }

