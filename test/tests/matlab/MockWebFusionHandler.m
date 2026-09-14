classdef MockWebFusionHandler < WebFusionHandler
    % Provide deterministic decoded API responses without network or a database.
    % Attributes: Payload (decoded JSON), Requests (method/path/filters records).
    properties
        Payload = struct()
        Requests = struct('method', {}, 'path', {}, 'filters', {})
    end
    methods
        function obj = MockWebFusionHandler(varargin)
            % Construct an offline client; Args: varargin: client name/value options.
            % Returns: obj: MockWebFusionHandler with initialization disabled.
            obj@WebFusionHandler(struct(), 'Initialize', false, varargin{:});
        end
    end
    methods (Access = protected)
        function payload = requestJSON(obj, method, path, filters)
            % Record an API request and return the fixture as decoded JSON.
            % Args: obj: mock; method/path: text; filters: scalar struct.
            % Returns: payload: fixture object; no HTTP is performed.
            obj.Requests(end+1) = struct('method', method, 'path', path, 'filters', filters);
            payload = obj.Payload;
        end
    end
end
