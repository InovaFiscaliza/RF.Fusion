classdef TestWebFusionHandler < matlab.unittest.TestCase
    % Verify the MATLAB compatibility boundary with decoded HTTP fixtures.
    methods (Test)
        function baseUrls(testCase)
            % Args: testCase: test instance. Returns: None; checks explicit endpoint selection.
            obj = MockWebFusionHandler('BaseUrl', [WebFusionHandler.INTERNAL_BASE_URL '/']);
            testCase.verifyEqual(obj.BaseUrl, WebFusionHandler.INTERNAL_BASE_URL);
            testCase.verifyEmpty(obj.Requests);
            settings.context.REPOSFI.WebFusionBaseUrl = WebFusionHandler.INTERNAL_HOST_BASE_URL;
            obj = WebFusionHandler(settings, 'Initialize', false);
            testCase.verifyEqual(obj.BaseUrl, WebFusionHandler.INTERNAL_HOST_BASE_URL);
        end

        function tableTypesAndCache(testCase)
            % Args: testCase: test instance. Returns: None; verifies null/date/text fidelity and caching.
            obj = MockWebFusionHandler();
            obj.Payload = jsondecode(['{"columns":["ID_FILE","NA_FILE","DT_TIME_START"],"rows":[' ...
                '{"ID_FILE":1,"NA_FILE":"001.bin","DT_TIME_START":"2026-09-11T12:34:56"},' ...
                '{"ID_FILE":null,"NA_FILE":null,"DT_TIME_START":null}]}']);
            rows = obj.getSpectrumFileData(struct('pageSize', 1));
            testCase.verifyEqual(height(rows), 2); % Preserve the next-page lookahead row.
            testCase.verifyEqual(rows.NA_FILE(1), "001.bin");
            testCase.verifyTrue(ismissing(rows.NA_FILE(2)));
            testCase.verifyTrue(isnan(rows.ID_FILE(2)));
            testCase.verifyTrue(isnat(rows.DT_TIME_START(2)));
            testCase.verifyEqual(rows.DT_TIME_START(1), datetime(2026,9,11,12,34,56));
            testCase.verifyEqual(obj.getSpectrumFileData(struct('pageSize',1)), rows);
            testCase.verifyEqual(numel(obj.Requests), 1);
            testCase.verifyEqual(obj.Requests.path, '/api/appanalise/files');
            testCase.verifyEqual(obj.Requests.method, 'POST');
        end

        function emptyTypedColumns(testCase)
            % Args: testCase: test instance. Returns: None; checks stable empty table schema.
            obj = MockWebFusionHandler();
            obj.Payload = jsondecode('{"columns":["ID_DISTRICT","LOCALITY_LABEL","DATE_START"],"rows":[]}');
            rows = obj.getSpectrumLocalities(NaN, struct('stateCode','SP'));
            testCase.verifySize(rows, [0 3]);
            testCase.verifyClass(rows.ID_DISTRICT, 'double');
            testCase.verifyClass(rows.LOCALITY_LABEL, 'string');
            testCase.verifyClass(rows.DATE_START, 'datetime');
        end

        function filtersAndLocalityScope(testCase)
            % Args: testCase: test instance. Returns: None; verifies absent values, dates and vectors.
            obj = MockWebFusionHandler();
            obj.Payload = jsondecode('{"columns":["LC_STATE"],"rows":[]}');
            filters = struct('equipmentId', 7, 'siteId', NaN, 'districtId', [2 3], ...
                'startDate', datetime(2026,9,11,14,30,0), 'endDate', NaT, ...
                'description', "A'B_%", 'stateCode', ' SP ');
            obj.getSpectrumLocalities(9, filters);
            sent = obj.Requests.filters;
            testCase.verifyEqual(sent.equipmentId, 9);
            testCase.verifyEqual(sent.districtId, [2 3]);
            testCase.verifyEqual(sent.startDate, '2026-09-11');
            testCase.verifyEqual(sent.description, 'A''B_%');
            testCase.verifyEqual(sent.stateCode, 'SP');
            testCase.verifyFalse(isfield(sent, 'siteId'));
            testCase.verifyFalse(isfield(sent, 'endDate'));
            testCase.verifyEmpty(obj.getSpectrumLocalities(NaN));
            testCase.verifyEqual(numel(obj.Requests), 1);
            testCase.verifyEqual(obj.computeFilterHash(struct('siteId',2,'equipmentId',1)), ...
                obj.computeFilterHash(struct('equipmentId',1,'siteId',2)));
        end

        function pathsAndCount(testCase)
            % Args: testCase: test instance. Returns: None; checks GET paths and scalar count cache.
            obj = MockWebFusionHandler();
            obj.Payload = jsondecode('{"columns":["ID_HOST","NA_HOST_PORT"],"rows":[{"ID_HOST":42,"NA_HOST_PORT":22}]}');
            host = obj.getHostStats(42);
            testCase.verifyEqual(host.NA_HOST_PORT, 22);
            obj.getSpectraByFileId(81);
            obj.getStationSummary();
            obj.getSpectrumEquipments();
            obj.getSpectrumStates();
            testCase.verifyEqual({obj.Requests.path}, {'/api/appanalise/hosts/42/stats', ...
                '/api/appanalise/files/81/spectra', '/api/appanalise/stations/summary', ...
                '/api/appanalise/equipments', '/api/appanalise/states'});
            obj.Payload = struct('count', 0);
            testCase.verifyEqual(obj.getSpectrumFileDataCount(), 0);
            testCase.verifyEqual(obj.getSpectrumFileDataCount(), 0);
            testCase.verifyEqual(numel(obj.Requests), 6);
        end

        function mapAndLegacyAdapters(testCase)
            % Args: testCase: test instance. Returns: None; checks shared map shape and HTTP dates.
            obj = MockWebFusionHandler();
            station = struct('equipment_id',1,'equipment_name','Radio','host_id',[], ...
                'host_name',[],'is_offline',[],'is_current_location',false,'map_state','no_host');
            point = struct('site_id',7,'site_label','Site 7','county_name',[], ...
                'district_id',[],'district_name',[],'state_id',[],'state_name',[], ...
                'state_code',[],'latitude',-10,'longitude',-50,'altitude',[], ...
                'gnss_measurements',[],'stations',station,'station_names',{{'Radio'}}, ...
                'marker_state','no_host','has_online_station',false, ...
                'has_online_host',false,'has_known_host',false);
            station.first_seen_at = 'Fri, 11 Sep 2026 12:30:00 GMT';
            station.last_seen_at = [];
            station.spectrum_count = 2;
            detail = struct('site_id',7,'stations',station,'marker_state','no_host', ...
                'has_online_station',false,'has_online_host',false,'has_known_host',false);
            obj.Payload = struct('points',point,'site_details',detail);
            [points, details] = obj.getMapDataSet();
            testCase.verifyEqual(obj.Requests.path, '/api/map/stations?include_details=true');
            testCase.verifyEqual(points.station_names, "Radio");
            testCase.verifyEmpty(points.stations.host_id);
            testCase.verifyEqual(details.stations.first_seen_at, datetime(2026,9,11,12,30,0));
            rows = obj.getSummarySiteRows();
            testCase.verifyEqual(rows.ID_SITE, 7);
            rows = obj.getSummaryStationRows();
            testCase.verifyEqual(rows.FIRST_SEEN_AT, datetime(2026,9,11,12,30,0));
            testCase.verifyTrue(isnan(rows.ID_HOST));
            testCase.verifyTrue(isnat(rows.LAST_SEEN_AT));
        end

        function emptyMap(testCase)
            % Args: testCase: test instance. Returns: None; checks empty map models and table aliases.
            obj = MockWebFusionHandler();
            obj.Payload = jsondecode('{"points":[],"site_details":[]}');
            [points, details] = obj.getMapDataSet();
            testCase.verifySize(points, [0 1]);
            testCase.verifySize(details, [0 1]);
            testCase.verifyTrue(isfield(points, 'station_names'));
            testCase.verifySize(obj.getSummarySiteRows(), [0 16]);
            testCase.verifySize(obj.getSummaryStationRows(), [0 11]);
        end

        function diskCacheScope(testCase)
            % Args: testCase: test instance. Returns: None; checks cache roundtrip and endpoint isolation.
            folder = tempname;
            mkdir(folder);
            cleanup = onCleanup(@() rmdir(folder, 's')); %#ok<NASGU>
            obj = MockWebFusionHandler('CacheFolder', folder);
            obj.CacheUpdatedAt = datestr(now, 'HH:MM:SS dd/mm/yyyy');
            obj.saveCache();
            testCase.verifyTrue(obj.isCacheValid());
            obj.getCache();
            other = MockWebFusionHandler('CacheFolder', folder, ...
                'BaseUrl', WebFusionHandler.INTERNAL_BASE_URL);
            testCase.verifyFalse(other.isCacheValid());
            testCase.verifyError(@() other.getCache(), 'WebFusionHandler:CacheMismatch');
        end

        function invalidResponseIsNotEmptySuccess(testCase)
            % Args: testCase: test instance. Returns: None; checks malformed payload rejection.
            obj = MockWebFusionHandler();
            obj.Payload = struct('unexpected', true);
            testCase.verifyError(@() obj.getSpectrumStates(), 'WebFusionHandler:InvalidResponse');
            testCase.verifyEmpty(obj.CacheSession);
            testCase.verifyError(@() obj.getMapDataSet(), 'WebFusionHandler:InvalidResponse');
            testCase.verifyError(@() obj.getSpectrumFileDataCount(), 'WebFusionHandler:InvalidResponse');
        end
    end
end
