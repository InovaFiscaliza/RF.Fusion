(function () {
    "use strict";

    /*
     * Home page station map controller.
     *
     * Why this file exists:
     *   - keep `index.html` focused on markup/CSS
     *   - isolate the map behavior in one maintainable place
     *   - document the small UX rules that would otherwise be buried in inline JS
     *
     * Data flow:
     *   1. `/api/map/stations` returns a lightweight point list for first paint
     *   2. the map renders markers, filters, and legend from that snapshot
     *   3. `/api/map/stations/<site_id>` is fetched lazily on hover to enrich the popup
     *
     * Important model nuance:
     *   - one plotted point represents one locality assumed by a station/site
     *   - the marker color does not mean "this coordinate is online right now" in all cases
     *   - color family encodes availability, while striped markers indicate
     *     that the point is historical rather than the current known locality
     */

    // This script is loaded only on the home page, but keep one cheap guard so
    // it fails silently if the template changes or the script is reused later.
    if (!window.L || !document.getElementById("station-map")) {
        return;
    }

    // ---------------------------------------------------------------------
    // DOM and map bootstrap
    // ---------------------------------------------------------------------
    const ALL_SITES_LABEL = "Todas as estações";
    const defaultCenter = [-14.2350, -51.9253];
    const brazilBounds = [[-35.5, -74.5], [7.5, -29.0]];
    const pointCount = document.getElementById("station-map-count");
    const clearFiltersButton = document.getElementById("station-map-clear-filters");
    const stateFilter = document.getElementById("station-map-state-filter");
    const siteFilter = document.getElementById("station-map-site-filter");
    const siteMenu = document.getElementById("station-map-site-menu");
    const themeSelect = document.getElementById("station-map-theme-select");
    const statusFilter = document.getElementById("station-map-status-filter");
    const localityFilter = document.getElementById("station-map-locality-filter");
    const startDateFilter = document.getElementById("station-map-start-date");
    const startDateNativePicker = document.getElementById("station-map-start-date-native");
    const endDateFilter = document.getElementById("station-map-end-date");
    const endDateNativePicker = document.getElementById("station-map-end-date-native");
    const legendContainer = document.getElementById("station-map-legend");
    const temporalFilterFields = [
        {
            queryKey: "start_date",
            textInput: startDateFilter,
            nativeInput: startDateNativePicker,
        },
        {
            queryKey: "end_date",
            textInput: endDateFilter,
            nativeInput: endDateNativePicker,
        },
    ];

    const map = L.map("station-map", {
        preferCanvas: true,
        zoomControl: true,
        scrollWheelZoom: true,
        minZoom: 3,
        maxBounds: brazilBounds,
        maxBoundsViscosity: 1.0,
        worldCopyJump: false
    }).setView(defaultCenter, 4);

    const markerLayer = L.layerGroup().addTo(map);
    const bounds = [];

    let allStationPoints = [];
    let renderedMarkers = [];
    let siteOptionRecords = [];
    let siteOptionIndex = new Map();
    let selectedSiteId = "";
    let currentBaseLayer = null;
    let currentOverlayLayer = null;
    let latestDatasetRequestId = 0;

    // External hover panel — singleton element that lives outside Leaflet's
    // clip boundary, so it never gets cropped by the map container.
    const hoverPanel = document.getElementById("station-hover-panel");
    let panelActiveMarker = null;

    // Keep the panel visible while the cursor travels from marker to panel.
    hoverPanel.addEventListener("mouseenter", () => {
        if (panelActiveMarker?.__wfCancelClose) panelActiveMarker.__wfCancelClose();
    });
    hoverPanel.addEventListener("mouseleave", () => {
        if (panelActiveMarker?.__wfScheduleClose) panelActiveMarker.__wfScheduleClose();
    });

    const MAP_THEME_STORAGE_KEY = "webfusion.station_map_theme";
    const POPUP_HOVER_OPEN_DELAY_MS = 140;
    const MARKER_FOCUS_ZOOM_THRESHOLD = 7;
    const MAX_NEARBY_POPUP_POINTS = 5;
    const DEFAULT_STATUS_FILTER_VALUE = "all";
    const DEFAULT_LOCALITY_FILTER_VALUE = "include_history";
    const POINT_STATE_ORDER = [
        "online_current",
        "online_previous",
        "offline_current",
        "offline_previous",
        "no_host"
    ];
    const HISTORICAL_POINT_STATES = new Set([
        "online_previous",
        "offline_previous",
    ]);
    const ONLINE_POINT_STATES = new Set([
        "online_current",
        "online_previous",
    ]);
    const OFFLINE_POINT_STATES = new Set([
        "offline_current",
        "offline_previous",
    ]);
    const POINT_STATE_SUMMARY_PRIORITY = {
        online_current: 0,
        online_previous: 1,
        offline_current: 2,
        offline_previous: 3,
        no_host: 4,
    };

    // Marker states come from the backend's map consolidation. The legend and
    // marker styling must stay aligned with those exact keys.
    const POINT_STATE_META = {
        online_current: {
            color: "#4f7f67",
            legendLabel: "Atual online",
            stripeColor: "rgba(56, 80, 63, 0.28)"
        },
        online_previous: {
            color: "#a7beab",
            legendLabel: "Histórico de estação online",
            stripeColor: "rgba(56, 80, 63, 0.32)"
        },
        offline_current: {
            color: "#b88352",
            legendLabel: "Atual offline",
            stripeColor: "rgba(112, 76, 45, 0.26)"
        },
        offline_previous: {
            color: "#dcc3ad",
            legendLabel: "Histórico de estação offline",
            stripeColor: "rgba(112, 76, 45, 0.24)"
        },
        no_host: {
            color: "#7b8aa0",
            legendLabel: "Sem host associado",
            stripeColor: "rgba(71, 85, 105, 0.22)"
        }
    };
    const MAP_THEMES = {
        classic: {
            label: "Clássico",
            url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            options: {
                maxZoom: 19,
                noWrap: true,
                attribution: "&copy; OpenStreetMap contributors"
            }
        },
        light: {
            label: "Claro",
            url: "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
            options: {
                subdomains: "abcd",
                maxZoom: 20,
                noWrap: true,
                attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
            }
        },
        dark: {
            label: "Escuro",
            url: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
            options: {
                subdomains: "abcd",
                maxZoom: 20,
                noWrap: true,
                attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
            }
        },
        satellite: {
            label: "Satélite",
            url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            options: {
                maxZoom: 19,
                noWrap: true,
                attribution: "Tiles &copy; Esri"
            },
            overlay: {
                url: "https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
                options: {
                    maxZoom: 19,
                    noWrap: true,
                    attribution: "Labels &copy; Esri"
                }
            }
        }
    };

    // ---------------------------------------------------------------------
    // Theme persistence and layer switching
    // ---------------------------------------------------------------------
    // Themes are intentionally simple basemap presets. Only satellite carries
    // a second overlay layer because imagery without labels proved too opaque
    // for operational navigation.
    /**
     * Read the persisted basemap choice from local storage.
     *
     * The map always falls back to `classic` when storage is unavailable or
     * when an older saved key no longer matches the supported theme catalog.
     */
    function loadSavedMapTheme() {
        try {
            const savedTheme = window.localStorage.getItem(MAP_THEME_STORAGE_KEY);
            if (savedTheme && MAP_THEMES[savedTheme]) {
                return savedTheme;
            }
        } catch (error) {
            // Storage can be unavailable in some browsing modes.
        }

        return "classic";
    }

    /**
     * Persist the chosen theme for the next visit.
     *
     * Failure to write must never block theme switching in the current page,
     * so storage errors are intentionally swallowed.
     */
    function saveMapTheme(themeKey) {
        try {
            window.localStorage.setItem(MAP_THEME_STORAGE_KEY, themeKey);
        } catch (error) {
            // Ignore storage failures; the theme still applies in-session.
        }
    }

    /**
     * Swap the active basemap and its optional overlay.
     *
     * Satellite is modeled as a two-layer theme because imagery without labels
     * proved hard to use operationally. Every other theme is a single tile
     * layer. The function is idempotent so repeated calls stay safe.
     */
    function applyMapTheme(themeKey) {
        // Keep theme switching idempotent: always remove the previous base
        // layer and optional overlay before mounting the new combination.
        const normalizedThemeKey = MAP_THEMES[themeKey] ? themeKey : "classic";
        const theme = MAP_THEMES[normalizedThemeKey];

        if (currentBaseLayer) {
            map.removeLayer(currentBaseLayer);
        }

        if (currentOverlayLayer) {
            map.removeLayer(currentOverlayLayer);
            currentOverlayLayer = null;
        }

        currentBaseLayer = L.tileLayer(theme.url, theme.options).addTo(map);

        if (theme.overlay) {
            currentOverlayLayer = L.tileLayer(
                theme.overlay.url,
                theme.overlay.options
            ).addTo(map);
        }

        if (themeSelect) {
            themeSelect.value = normalizedThemeKey;
        }

        saveMapTheme(normalizedThemeKey);
    }

    // ---------------------------------------------------------------------
    // Small formatting helpers
    // ---------------------------------------------------------------------
    /**
     * Escape values interpolated into popup/menu HTML strings.
     *
     * This script still renders several small HTML fragments manually, so all
     * user/data-derived text goes through this helper before interpolation.
     */
    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/\"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    /**
     * Normalize free-text search into an accent-insensitive lowercase token.
     *
     * This keeps the station combobox usable for Brazilian locality names
     * without forcing exact punctuation or diacritic matches.
     */
    function normalizeSearchText(value) {
        return String(value || "")
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "")
            .trim()
            .toLowerCase();
    }

    /**
     * Return true only when two textual labels are meaningfully different.
     *
     * Several locality fields may repeat the same semantic label in slightly
     * different forms; this helper avoids noisy duplicated suffixes.
     */
    function isDistinctText(value, reference) {
        return Boolean(value) && normalizeSearchText(value) !== normalizeSearchText(reference);
    }

    /**
     * Build the search corpus for one plotted point.
     *
     * The combobox intentionally searches across locality labels and known
     * station/equipment names, so users can find a point either by geography
     * or by the station they remember.
     */
    function getPointSearchText(point) {
        // Search intentionally indexes both the locality fields and the
        // station/equipment names so the combobox can behave more like a fuzzy
        // operational search box than a strict site-id picker.
        const parts = [
            point.site_label,
            point.county_name,
            point.district_name,
            getPointDisplayName(point)
        ];

        if (Array.isArray(point.stations)) {
            point.stations.forEach((station) => {
                parts.push(station.host_name || station.equipment_name || "");
            });
        }

        if (Array.isArray(point.station_names)) {
            point.station_names.forEach((stationName) => {
                parts.push(stationName || "");
            });
        }

        return normalizeSearchText(parts.join(" "));
    }

    /**
     * Collect every searchable station alias attached to one point.
     *
     * The map groups colocated stations into one point, but the combobox must
     * still surface those individual station names when the user searches by
     * station prefix.
     */
    function getPointSearchAliases(point) {
        const aliases = [];

        if (Array.isArray(point.stations)) {
            point.stations.forEach((station) => {
                aliases.push(station.host_name || station.equipment_name || "");
            });
        }

        if (Array.isArray(point.station_names)) {
            point.station_names.forEach((stationName) => {
                aliases.push(stationName || "");
            });
        }

        return [...new Set(
            aliases
                .map((alias) => String(alias || "").trim())
                .filter(Boolean)
        )].sort(compareDisplayText);
    }

    /**
     * Build the station-facing combobox label used during free-text matches.
     *
     * When the user searches by station prefix we prefer the matched station
     * name itself, adding geography only as a compact disambiguator.
     */
    function buildStationMatchLabel(point, stationLabel) {
        const countyState = getPointCountyStateLabel(point);

        if (countyState) {
            return `${stationLabel} · ${countyState}`;
        }

        return stationLabel;
    }

    /**
     * Expand the point-based option list into station-specific matches.
     *
     * This keeps the combobox aligned with the free-text map filter: typing a
     * station prefix should show matching station names, not the grouped point
     * title chosen for the marker popup.
     */
    function getStationMatchOptions(searchTerm) {
        if (!searchTerm) {
            return [];
        }

        const stationOptions = [];

        siteOptionRecords.forEach((option) => {
            if (!option.site_id || !option.point) {
                return;
            }

            getPointSearchAliases(option.point).forEach((alias) => {
                if (!normalizeSearchText(alias).includes(searchTerm)) {
                    return;
                }

                stationOptions.push({
                    site_id: option.site_id,
                    label: buildStationMatchLabel(option.point, alias),
                });
            });
        });

        return stationOptions.sort((optionA, optionB) => {
            const labelDiff = compareDisplayText(optionA.label, optionB.label);

            if (labelDiff !== 0) {
                return labelDiff;
            }

            return Number(optionA.site_id || 0) - Number(optionB.site_id || 0);
        });
    }

    /**
     * Format the compact `county/state` label used across the map UI.
     *
     * This is the shortest stable geography label available for tooltips,
     * option labels, and popup summaries.
     */
    function getPointCountyStateLabel(point) {
        if (point.county_name && point.state_code) {
            return `${point.county_name}/${point.state_code}`;
        }

        if (point.county_name) {
            return point.county_name;
        }

        return point.state_code || "";
    }

    /**
     * Build the richer locality label shown in popups.
     *
     * The function prefers a composed `site · county/state` label, but avoids
     * repeating the county name when the site label already equals it.
     */
    function getPointLocalityLabel(point) {
        const countyState = getPointCountyStateLabel(point);

        if (point.site_label && countyState && point.site_label !== point.county_name) {
            return `${point.site_label} · ${countyState}`;
        }

        if (point.site_label && !countyState) {
            return point.site_label;
        }

        return countyState || point.site_label;
    }

    /**
     * Build the shorter locality subtitle used in the hover panel hierarchy.
     *
     * The state already appears as the primary heading there, so this label
     * intentionally avoids repeating the state code when a more local place
     * name is available.
     */
    function getPointLocalityShortLabel(point) {
        const siteLabel = String(point?.site_label || "").trim();
        const districtName = String(point?.district_name || "").trim();
        const countyName = String(point?.county_name || "").trim();

        if (siteLabel && countyName && isDistinctText(siteLabel, countyName)) {
            return `${siteLabel} · ${countyName}`;
        }

        if (siteLabel) {
            return siteLabel;
        }

        if (districtName && isDistinctText(districtName, countyName)) {
            return districtName;
        }

        return countyName || "";
    }

    /**
     * Resolve the primary state heading shown in map hover panels.
     */
    function getPointStateDisplayLabel(point) {
        return String(point?.state_name || point?.state_code || "Estado não identificado").trim();
    }

    /**
     * Compare user-facing labels with accent/case tolerance.
     */
    function compareDisplayText(textA, textB) {
        return String(textA || "").localeCompare(String(textB || ""), "pt-BR", {
            sensitivity: "base",
            numeric: true,
        });
    }

    /**
     * Resolve the compact station/equipment label for ordering and display.
     */
    function getStationDisplayName(station) {
        return String(station?.host_name || station?.equipment_name || "").trim();
    }

    /**
     * Resolve the first alphabetical station label associated with one point.
     */
    function getPointPrimaryStationLabel(point) {
        const stationLabels = Array.isArray(point?.stations) && point.stations.length > 0
            ? point.stations.map(getStationDisplayName).filter(Boolean)
            : Array.isArray(point?.station_names)
            ? point.station_names.map((name) => String(name || "").trim()).filter(Boolean)
            : [];

        if (!stationLabels.length) {
            return "";
        }

        return [...stationLabels].sort(compareDisplayText)[0];
    }

    /**
     * Produce the human-facing point label used in tooltips and selectors.
     *
     * When multiple stations share one plotted locality we intentionally
     * collapse the label to `first (+N)` so the map stays scannable.
     */
    function getPointDisplayName(point) {
        const namedStations = Array.isArray(point.stations) && point.stations.length > 0
            ? point.stations
                .map((station) => getStationDisplayName(station))
                .filter(Boolean)
            : Array.isArray(point.station_names)
            ? point.station_names.filter(Boolean)
            : [];
        const orderedStationNames = [...namedStations].sort(compareDisplayText);

        if (orderedStationNames.length === 1) {
            return orderedStationNames[0];
        }

        if (orderedStationNames.length > 1) {
            return `${orderedStationNames[0]} (+${orderedStationNames.length - 1})`;
        }

        return point.site_label;
    }

    /**
     * Order station entries alphabetically for hover-panel readability.
     */
    function comparePopupStationOrder(stationA, stationB) {
        return compareDisplayText(
            getStationDisplayName(stationA),
            getStationDisplayName(stationB),
        );
    }

    /**
     * Order popup points by state, then locality, then station label.
     */
    function comparePopupPointOrder(pointA, pointB) {
        const stateDiff = compareDisplayText(
            getPointStateDisplayLabel(pointA),
            getPointStateDisplayLabel(pointB),
        );
        if (stateDiff !== 0) {
            return stateDiff;
        }

        const localityDiff = compareDisplayText(
            getPointLocalityShortLabel(pointA) || getPointLocalityLabel(pointA),
            getPointLocalityShortLabel(pointB) || getPointLocalityLabel(pointB),
        );
        if (localityDiff !== 0) {
            return localityDiff;
        }

        const stationDiff = compareDisplayText(
            getPointPrimaryStationLabel(pointA),
            getPointPrimaryStationLabel(pointB),
        );
        if (stationDiff !== 0) {
            return stationDiff;
        }

        return (pointA.site_id || 0) - (pointB.site_id || 0);
    }

    /**
     * Format the station-count badge with the correct Portuguese plural.
     */
    function formatStationCountLabel(stationCount) {
        return `${stationCount} ${stationCount === 1 ? "estação" : "estações"}`;
    }

    /**
     * Build the base site-combobox label before duplicate disambiguation.
     *
     * The option list starts from the station-facing display name and appends
     * geography only once, keeping the selector compact but still searchable.
     */
    function getPointOptionBaseLabel(point) {
        const pointLabel = getPointDisplayName(point);
        const countyState = getPointCountyStateLabel(point);

        if (countyState) {
            return `${pointLabel} · ${countyState}`;
        }

        return pointLabel;
    }

    /**
     * Pick the least noisy suffix to separate duplicate combobox labels.
     *
     * Duplicate labels happen when one station has multiple historical
     * localities or when locality names collide. We prefer real geography
     * before falling back to a synthetic site id.
     */
    function getPointOptionDisambiguator(point) {
        if (isDistinctText(point.district_name, point.county_name)) {
            return point.district_name;
        }

        if (isDistinctText(point.site_label, point.county_name) && isDistinctText(point.site_label, point.district_name)) {
            return point.site_label;
        }

        return `ponto ${point.site_id}`;
    }

    /**
     * Return the currently selected site id from the combobox state.
     */
    function getSelectedSiteId() {
        return selectedSiteId;
    }

    /**
     * Left-pad numeric date parts so UI formatting stays stable.
     */
    function padDateNumber(value, size = 2) {
        return String(value).padStart(size, "0");
    }

    /**
     * Tell whether one `(year, month, day)` triple is a real calendar date.
     */
    function isValidDateParts(year, month, day) {
        if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
            return false;
        }

        const candidate = new Date(Date.UTC(year, month - 1, day));
        return (
            candidate.getUTCFullYear() === year
            && candidate.getUTCMonth() === month - 1
            && candidate.getUTCDate() === day
        );
    }

    /**
     * Format an ISO date as `DD/MM/YYYY` for the manual text inputs.
     */
    function formatIsoDateForDisplay(isoValue) {
        const match = String(isoValue || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);

        if (!match) {
            return "";
        }

        return `${match[3]}/${match[2]}/${match[1]}`;
    }

    /**
     * Build one normalized ISO date from already-validated parts.
     */
    function buildIsoDateValue(year, month, day) {
        return [
            padDateNumber(year, 4),
            padDateNumber(month),
            padDateNumber(day),
        ].join("-");
    }

    /**
     * Parse one manual map-date field, accepting Brazilian and ISO formats.
     *
     * Native `type="date"` controls segment the input in some browsers, which
     * made manual typing frustrating. We now accept plain text and normalize
     * it only when the user commits the field.
     */
    function normalizeManualDateValue(value) {
        const rawValue = String(value || "").trim();

        if (!rawValue) {
            return {
                rawValue,
                isoValue: "",
                displayValue: "",
                isEmpty: true,
                isValid: true,
            };
        }

        const compactDigits = rawValue.replace(/\D/g, "");
        let year = null;
        let month = null;
        let day = null;
        let match = rawValue.match(/^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$/);

        if (match) {
            year = Number(match[1]);
            month = Number(match[2]);
            day = Number(match[3]);
        } else {
            match = rawValue.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{4})$/);

            if (match) {
                day = Number(match[1]);
                month = Number(match[2]);
                year = Number(match[3]);
            } else if (compactDigits.length === 8) {
                day = Number(compactDigits.slice(0, 2));
                month = Number(compactDigits.slice(2, 4));
                year = Number(compactDigits.slice(4, 8));
            }
        }

        if (!isValidDateParts(year, month, day)) {
            return {
                rawValue,
                isoValue: "",
                displayValue: rawValue,
                isEmpty: false,
                isValid: false,
            };
        }

        const isoValue = buildIsoDateValue(year, month, day);

        return {
            rawValue,
            isoValue,
            displayValue: formatIsoDateForDisplay(isoValue),
            isEmpty: false,
            isValid: true,
        };
    }

    /**
     * Clear temporary invalid styling while the user is still typing.
     */
    function clearDateInputValidation(input) {
        if (!input) {
            return;
        }

        input.classList.remove("is-invalid");
        input.removeAttribute("aria-invalid");
        input.setCustomValidity("");
    }

    /**
     * Keep plain-digit typing comfortable without rejecting slash-based input.
     *
     * If the user types only digits we progressively format as `DD/MM/YYYY`.
     * When separators are already present, we keep the raw shape and only
     * normalize on commit so `01/01/2025` and `2025-01-01` both stay usable.
     */
    function formatManualDateValueWhileTyping(value) {
        const rawValue = String(value || "").replace(/\s+/g, "");

        if (!rawValue) {
            return "";
        }

        if (/[./-]/.test(rawValue)) {
            return rawValue.slice(0, 10);
        }

        const digits = rawValue.replace(/\D/g, "").slice(0, 8);

        if (!digits) {
            return "";
        }

        if (digits.length <= 2) {
            return digits;
        }

        if (digits.length <= 4) {
            return `${digits.slice(0, 2)}/${digits.slice(2)}`;
        }

        return `${digits.slice(0, 2)}/${digits.slice(2, 4)}/${digits.slice(4)}`;
    }

    /**
     * Push one canonical ISO date into both temporal widgets.
     */
    function setTemporalFieldIsoValue(field, isoValue) {
        if (!field) {
            return;
        }

        if (field.textInput) {
            field.textInput.value = formatIsoDateForDisplay(isoValue);
            field.textInput.dataset.isoValue = isoValue || "";
            clearDateInputValidation(field.textInput);
        }

        if (field.nativeInput) {
            field.nativeInput.value = isoValue || "";
        }
    }

    /**
     * Normalize one temporal field and keep both widgets synchronized.
     */
    function syncTemporalField(field, options = {}) {
        const input = field?.textInput;
        const normalizedDate = normalizeManualDateValue(input ? input.value : "");

        if (!input) {
            return normalizedDate;
        }

        input.dataset.isoValue = normalizedDate.isoValue;
        input.classList.toggle("is-invalid", !normalizedDate.isValid && !normalizedDate.isEmpty);

        if (!normalizedDate.isValid && !normalizedDate.isEmpty) {
            input.setAttribute("aria-invalid", "true");
            input.setCustomValidity("Use o formato dd/mm/aaaa ou aaaa-mm-dd.");
        } else {
            input.removeAttribute("aria-invalid");
            input.setCustomValidity("");

            if (options.syncDisplay && input.value !== normalizedDate.displayValue) {
                input.value = normalizedDate.displayValue;
            }
        }

        if (field.nativeInput) {
            field.nativeInput.value = normalizedDate.isValid ? normalizedDate.isoValue : "";
        }

        return normalizedDate;
    }

    /**
     * Keep the visible text field pleasant while the user is still typing.
     */
    function handleTemporalFieldTyping(field) {
        if (!field?.textInput) {
            return;
        }

        const formattedValue = formatManualDateValueWhileTyping(field.textInput.value);

        if (field.textInput.value !== formattedValue) {
            field.textInput.value = formattedValue;
        }

        clearDateInputValidation(field.textInput);

        if (field.nativeInput) {
            field.nativeInput.value = normalizeManualDateValue(field.textInput.value).isoValue;
        }

        updateClearFiltersButtonState();
    }

    /**
     * Return the normalized state of all temporal filters in one pass.
     */
    function syncTemporalFields(options = {}) {
        return temporalFilterFields.map((field) => ({
            field,
            state: syncTemporalField(field, options),
        }));
    }

    /**
     * Tell whether any non-theme map filter currently deviates from defaults.
     */
    function hasActiveMapFilters() {
        return Boolean(
            (stateFilter && stateFilter.value)
            || (siteFilter && String(siteFilter.value || "").trim())
            || (statusFilter && statusFilter.value !== DEFAULT_STATUS_FILTER_VALUE)
            || (localityFilter && localityFilter.value !== DEFAULT_LOCALITY_FILTER_VALUE)
            || temporalFilterFields.some((field) => String(field?.textInput?.value || "").trim())
        );
    }

    /**
     * Keep the clear-filters button enabled only when it has work to do.
     */
    function updateClearFiltersButtonState() {
        if (!clearFiltersButton) {
            return;
        }

        clearFiltersButton.disabled = !hasActiveMapFilters();
    }

    /**
     * Normalize both temporal fields and apply the map filter when valid.
     */
    function commitTemporalFilterChange() {
        const temporalStates = syncTemporalFields({syncDisplay: true});
        const invalidEntry = temporalStates.find(({state}) => !state.isValid && !state.isEmpty);

        updateClearFiltersButtonState();

        if (
            invalidEntry
            && invalidEntry.field.textInput
            && typeof invalidEntry.field.textInput.reportValidity === "function"
        ) {
            invalidEntry.field.textInput.reportValidity();
            return;
        }

        loadStationPoints();
    }

    /**
     * Restore the operational map filters to their initial all-data state.
     *
     * Theme is intentionally preserved because it behaves like a shell/user
     * preference rather than a data filter.
     */
    function clearMapFilters() {
        const hadTemporalFilters = temporalFilterFields.some((field) =>
            String(field?.textInput?.value || "").trim()
        );

        if (stateFilter) {
            stateFilter.value = "";
        }

        selectedSiteId = "";

        if (siteFilter) {
            siteFilter.value = "";
        }

        closeSiteMenu();

        if (statusFilter) {
            statusFilter.value = DEFAULT_STATUS_FILTER_VALUE;
        }

        if (localityFilter) {
            localityFilter.value = DEFAULT_LOCALITY_FILTER_VALUE;
        }

        temporalFilterFields.forEach((field) => setTemporalFieldIsoValue(field, ""));

        if (allStationPoints.length > 0) {
            populateSiteFilter(allStationPoints, "");
        }

        updateClearFiltersButtonState();

        if (hadTemporalFilters) {
            loadStationPoints();
            return;
        }

        renderFilteredPoints();
    }

    /**
     * Normalize the optional temporal controls into query parameters.
     *
     * Once the manual fields are parsed, ISO lexical order is still safe when
     * we need to repair an inverted range quickly in the UI.
     */
    function getTemporalFilterQueryParams() {
        const temporalStates = syncTemporalFields({syncDisplay: true});
        let startDate = temporalStates[0].state.isoValue;
        let endDate = temporalStates[1].state.isoValue;

        if (startDate && endDate && startDate > endDate) {
            [startDate, endDate] = [endDate, startDate];
            setTemporalFieldIsoValue(temporalFilterFields[0], startDate);
            setTemporalFieldIsoValue(temporalFilterFields[1], endDate);
        }

        const params = new URLSearchParams();
        const temporalValues = [startDate, endDate];

        temporalFilterFields.forEach((field, index) => {
            if (temporalValues[index]) {
                params.set(field.queryKey, temporalValues[index]);
            }
        });

        return params;
    }

    /**
     * Append the active temporal filter to one map API path.
     */
    function buildMapApiUrl(pathname) {
        const params = getTemporalFilterQueryParams();
        const queryString = params.toString();
        return queryString ? `${pathname}?${queryString}` : pathname;
    }

    /**
     * Build the spectrum-page URL for one popup entry, preserving map dates.
     */
    function buildSpectrumHref(point, station) {
        if (
            !station
            || station.equipment_id === null
            || station.equipment_id === undefined
            || point.site_id === null
            || point.site_id === undefined
        ) {
            return null;
        }

        const params = getTemporalFilterQueryParams();
        params.set("equipment_id", String(station.equipment_id));
        params.set("site_id", String(point.site_id));
        params.set("sort_by", "recent");

        return `/spectrum?${params.toString()}`;
    }

    /**
     * Map backend station availability into the popup badge CSS class.
     */
    function stationStatusClass(station) {
        if (station.host_id === null || station.host_id === undefined) {
            return "station-status-unknown";
        }

        return station.is_offline ? "station-status-offline" : "station-status-online";
    }

    /**
     * Map backend station availability into the popup badge text.
     */
    function stationStatusLabel(station) {
        if (station.host_id === null || station.host_id === undefined) {
            return "Sem host";
        }

        return station.is_offline ? "Offline" : "Online";
    }

    /**
     * Match one station row against the free-text station filter.
     *
     * This keeps the "Estação" control intuitive when a locality contains
     * multiple colocated stations: once the user filters by one station name,
     * marker color and popup content should reflect that station subset rather
     * than the unrelated colocated peers.
     */
    function stationMatchesSearchTerm(station, searchTerm) {
        if (!searchTerm) {
            return false;
        }

        const corpus = normalizeSearchText([
            station?.host_name || "",
            station?.equipment_name || "",
        ].join(" "));

        return Boolean(corpus) && corpus.includes(searchTerm);
    }

    /**
     * Snapshot the active station-level scopes derived from the toolbar.
     */
    function buildActiveStationScope({ searchTerm = "", statusScope = "all", localityScope = "include_history" } = {}) {
        return {
            searchTerm: String(searchTerm || "").trim(),
            statusScope,
            localityScope,
        };
    }

    /**
     * Tell whether any station-level restriction is currently active.
     */
    function hasActiveStationScope(scope) {
        if (!scope) {
            return false;
        }

        return Boolean(
            scope.searchTerm
            || scope.statusScope === "online_only"
            || scope.statusScope === "offline_only"
            || scope.localityScope === "current_only"
            || scope.localityScope === "historical_only"
        );
    }

    /**
     * Tell whether one station survives the active station-level scopes.
     */
    function stationMatchesScope(station, scope) {
        if (!station) {
            return false;
        }

        if (scope?.searchTerm && !stationMatchesSearchTerm(station, scope.searchTerm)) {
            return false;
        }

        const stateKey = station.map_state || "no_host";

        if (scope?.statusScope === "online_only" && !ONLINE_POINT_STATES.has(stateKey)) {
            return false;
        }

        if (scope?.statusScope === "offline_only" && !OFFLINE_POINT_STATES.has(stateKey)) {
            return false;
        }

        if (scope?.localityScope === "current_only" && HISTORICAL_POINT_STATES.has(stateKey)) {
            return false;
        }

        if (scope?.localityScope === "historical_only" && !HISTORICAL_POINT_STATES.has(stateKey)) {
            return false;
        }

        return true;
    }

    /**
     * Filter one station array by the active station-level scopes.
     */
    function filterStationsByScope(stations, scope) {
        if (!Array.isArray(stations)) {
            return [];
        }

        if (!hasActiveStationScope(scope)) {
            return stations.map((station) => ({ ...station }));
        }

        return stations
            .filter((station) => stationMatchesScope(station, scope))
            .map((station) => ({ ...station }));
    }

    /**
     * Collapse a filtered station subset back into one marker-state key.
     */
    function summarizePointStateFromStations(stations) {
        if (!Array.isArray(stations) || stations.length === 0) {
            return "no_host";
        }

        let bestState = "no_host";
        let bestPriority = POINT_STATE_SUMMARY_PRIORITY[bestState];

        stations.forEach((station) => {
            const stateKey = station?.map_state || "no_host";
            const priority = POINT_STATE_SUMMARY_PRIORITY[stateKey] ?? POINT_STATE_SUMMARY_PRIORITY.no_host;

            if (priority < bestPriority) {
                bestState = stateKey;
                bestPriority = priority;
            }
        });

        return bestState;
    }

    /**
     * Clone one point so a station text filter can scope marker state and popup
     * content to the matching stations only, without mutating the base dataset.
     */
    function buildStationScopedPoint(point, stations, scope) {
        const scopedStations = Array.isArray(stations) ? stations : [];
        const activeStationScope = buildActiveStationScope(scope);

        return {
            ...point,
            stations: scopedStations,
            station_names: scopedStations
                .map((station) => station.host_name || station.equipment_name || "")
                .filter(Boolean),
            marker_state: summarizePointStateFromStations(scopedStations),
            has_online_station: scopedStations.some((station) =>
                ONLINE_POINT_STATES.has(station?.map_state || "no_host")
            ),
            has_online_host: scopedStations.some((station) =>
                ONLINE_POINT_STATES.has(station?.map_state || "no_host")
            ),
            has_known_host: scopedStations.some((station) =>
                station?.host_id !== null && station?.host_id !== undefined
            ),
            _activeStationScope: activeStationScope,
            _stationScopeSearchTerm: activeStationScope.searchTerm || "",
        };
    }

    /**
     * Return the normalized state key for one point.
     */
    function getPointStateKey(point) {
        return point.marker_state || "no_host";
    }

    /**
     * Resolve the marker metadata from the backend-provided point state.
     */
    function getPointStateMeta(point) {
        return POINT_STATE_META[getPointStateKey(point)] || POINT_STATE_META.no_host;
    }

    /**
     * Tell whether a marker state represents a historical locality rather than
     * the current known position of the station.
     */
    function isHistoricalMarkerState(stateKey) {
        return HISTORICAL_POINT_STATES.has(stateKey);
    }

    /**
     * Tell whether a plotted point should be treated as a historical locality.
     */
    function isHistoricalPoint(point) {
        return isHistoricalMarkerState(getPointStateKey(point));
    }

    /**
     * Tell whether the consolidated point state should be treated as online.
     */
    function isOnlinePoint(point) {
        return ONLINE_POINT_STATES.has(getPointStateKey(point));
    }

    /**
     * Tell whether the consolidated point state should be treated as offline.
     */
    function isOfflinePoint(point) {
        return OFFLINE_POINT_STATES.has(getPointStateKey(point));
    }

    /**
     * Resolve the compact locality-mode label shown in popup summaries.
     */
    function getPointLocationModeLabel(point) {
        if (getPointStateKey(point) === "no_host") {
            return "Sem host";
        }

        return isHistoricalPoint(point) ? "Histórico" : "Atual";
    }

    /**
     * Order marker rendering so current online points are painted last.
     *
     * We intentionally keep this as a simple insertion-order rule instead of
     * relying on pane-level priority tricks, because the latter made nearby
     * markers harder to interact with in dense areas.
     */
    function getPointRenderPriority(point) {
        const priority = {
            online_previous: 0,
            offline_previous: 1,
            no_host: 2,
            offline_current: 3,
            online_current: 4,
        };

        return priority[getPointStateKey(point)] ?? 99;
    }

    /**
     * Derive marker sizing from the current zoom level.
     *
     * The map should feel a bit more tactile as the user zooms in, but the
     * growth is deliberately capped so dense areas do not turn into blobs.
     */
    function getMarkerVisualMetrics() {
        /* Marker scaling is intentionally conservative.
         *
         * The map needs two simultaneous behaviors:
         * - at low zoom, stay readable without turning Brazil into a carpet of
         *   oversized dots
         * - at higher zoom, add enough contrast/halo/core detail so markers
         *   remain visible above terrain, vegetation and satellite textures
         *
         * This helper is the one place where that visual contract lives, so
         * vector markers and striped historical markers evolve together.
         */
        const zoom = map.getZoom();
        const isFocusZoom = zoom >= MARKER_FOCUS_ZOOM_THRESHOLD;
        const focusBoost = isFocusZoom ? Math.min(5.5, 1.8 + (zoom - MARKER_FOCUS_ZOOM_THRESHOLD) * 1.1) : 0;
        const iconSize = Math.max(18, Math.min(33, 18 + (zoom - 4) * 1.45 + focusBoost));
        const circleRadius = Math.max(8, Math.min(15.5, 8 + (zoom - 4) * 0.72 + (isFocusZoom ? Math.min(2.7, 0.9 + (zoom - MARKER_FOCUS_ZOOM_THRESHOLD) * 0.45) : 0)));
        const strokeWeight = Math.max(2, Math.min(4.2, 2 + (zoom - 4) * 0.1 + (isFocusZoom ? 0.9 : 0)));

        return {
            isFocusZoom,
            iconSize,
            iconAnchor: iconSize / 2,
            popupOffsetY: Math.round(iconSize * 0.75),
            circleRadius,
            strokeWeight,
            borderWidth: Math.max(2, Math.min(4.1, 2 + (zoom - 4) * 0.08 + (isFocusZoom ? 0.85 : 0))),
            haloWidth: isFocusZoom ? Math.min(5, 2.1 + (zoom - MARKER_FOCUS_ZOOM_THRESHOLD) * 0.55) : 0,
            outlineWidth: isFocusZoom ? 1.2 : 0,
            coreSize: isFocusZoom ? Math.max(4, Math.round(iconSize * 0.28)) : 0,
            focusShadowOpacity: isFocusZoom ? 0.16 : 0.08,
        };
    }

    /**
     * Build the inner HTML used by the custom div-based marker.
     *
     * We use an HTML marker instead of a simple vector circle so historical
     * localities can carry stripes without introducing a plugin dependency.
     */
    function buildMarkerHtml(point) {
        const stateMeta = getPointStateMeta(point);
        const metrics = getMarkerVisualMetrics();
        const historicalClass = isHistoricalPoint(point) ? " is-historical" : "";
        const focusClass = metrics.isFocusZoom ? " is-focus-zoom" : "";

        return `
            <span
                class="station-map-marker${historicalClass}${focusClass}"
                style="
                    --marker-fill: ${stateMeta.color};
                    --marker-stripe-color: ${stateMeta.stripeColor || "rgba(255, 255, 255, 0.4)"};
                    --marker-size: ${metrics.iconSize}px;
                    --marker-border-width: ${metrics.borderWidth}px;
                    --marker-halo-width: ${metrics.haloWidth}px;
                    --marker-outline-width: ${metrics.outlineWidth}px;
                    --marker-core-size: ${metrics.coreSize}px;
                    --marker-shadow-opacity: ${metrics.focusShadowOpacity};
                "
            ></span>
        `;
    }

    /**
     * Create the Leaflet icon for one point.
     */
    function createMarkerIcon(point) {
        const metrics = getMarkerVisualMetrics();
        return L.divIcon({
            className: "station-map-marker-icon",
            html: buildMarkerHtml(point),
            iconSize: [metrics.iconSize, metrics.iconSize],
            iconAnchor: [metrics.iconAnchor, metrics.iconAnchor],
        });
    }

    /**
     * Create the concrete Leaflet marker object for one point.
     *
     * All markers use the same `divIcon` pipeline so stacking follows the
     * insertion order consistently across current and historical localities.
     */
    function createLeafletMarker(point) {
        const marker = L.marker([point.latitude, point.longitude], {
            icon: createMarkerIcon(point),
            interactive: true,
            bubblingMouseEvents: false,
            riseOnHover: true,
        });
        marker.__wfMarkerMode = "icon";
        return marker;
    }

    /**
     * Reinforce DOM interactivity for `divIcon` markers.
     *
     * Wiring the DOM element directly keeps hover/click behavior predictable
     * even when markers are visually tight.
     */
    function ensureMarkerElementInteractivity(marker) {
        if (!marker || marker.__wfMarkerMode !== "icon") {
            return;
        }

        const element = marker.getElement();
        if (!element) {
            return;
        }

        element.style.pointerEvents = "auto";
        element.style.cursor = "pointer";

        const innerMarker = element.querySelector(".station-map-marker");
        if (innerMarker) {
            innerMarker.style.pointerEvents = "none";
        }

        if (element.dataset.wfMarkerHoverBound === "1") {
            return;
        }

        element.dataset.wfMarkerHoverBound = "1";
        element.addEventListener("mouseenter", () => marker.fire("mouseover"));
        element.addEventListener("mouseleave", () => marker.fire("mouseout"));
        element.addEventListener("click", () => showHoverPanel(marker));
    }

    /**
     * Measure the on-screen distance between two markers.
     *
     * Nearby-point grouping is decided in pixel space, not geographic
     * distance, because the ambiguity problem is visual: if two markers look
     * far apart on the screen, they no longer compete for the same hover
     * intent even when the coordinates are geographically close.
     */
    function getMarkerPixelDistance(markerA, markerB) {
        const pointA = map.latLngToContainerPoint(markerA.getLatLng());
        const pointB = map.latLngToContainerPoint(markerB.getLatLng());
        return pointA.distanceTo(pointB);
    }

    /**
     * Resolve the semantic group label used inside zoomed-out cluster popups.
     *
     * Site/locality labels are preferred because that is what operators tend
     * to recognize first when multiple nearby geopoints collapse visually.
     */
    function getClusterGroupDescriptor(point) {
        const stateLabel = getPointStateDisplayLabel(point);
        const localityLabel = getPointLocalityShortLabel(point) || getPointLocalityLabel(point) || `ID_SITE ${point.site_id}`;
        const stateKey = normalizeSearchText(stateLabel);
        const localityKey = normalizeSearchText(localityLabel);

        return {
            key: `${stateKey}|${localityKey || `site-${point.site_id}`}`,
            label: stateLabel,
            context: localityLabel,
            stateLabel,
            localityLabel,
        };
    }

    /**
     * Resolve the clustering radius used by the zoom-aware popup summary.
     *
     * The lower the zoom, the more points appear visually glued together, so
     * the popup can summarize a slightly wider on-screen neighborhood.
     */
    function getClusterPopupDistancePx() {
        const zoom = map.getZoom();

        if (zoom <= 4) {
            return 150;
        }

        if (zoom === 5) {
            return 122;
        }

        if (zoom === 6) {
            return 96;
        }

        if (zoom === 7) {
            return 74;
        }

        if (zoom === 8) {
            return 58;
        }

        return 42;
    }

    /**
     * Decide how many points the zoomed-out cluster popup should list.
     */
    function getNearbyPopupMaxItems() {
        return MAX_NEARBY_POPUP_POINTS;
    }

    /**
     * Sort one popup-cluster point so the most operationally relevant entries
     * appear first inside each locality group.
     */
    function compareClusterPointPriority(pointA, pointB) {
        return comparePopupPointOrder(pointA, pointB);
    }

    /**
     * Build the zoom-aware popup-cluster summary for one hovered marker.
     *
     * At long zooms the popup intentionally summarizes multiple nearby points
     * grouped by locality. As the user zooms in and those points separate on
     * screen, the popup naturally falls back to the detailed single-point view.
     */
    function buildNearbyPopupSummary(activeMarker) {
        const maxDistance = getClusterPopupDistancePx();
        const clusteredEntries = renderedMarkers
            .map((marker) => ({
                marker,
                point: marker.__wfPoint || {},
                distance: marker === activeMarker ? 0 : getMarkerPixelDistance(activeMarker, marker),
            }))
            .filter((entry) => entry.marker === activeMarker || entry.distance <= maxDistance)
            .sort((entryA, entryB) => compareClusterPointPriority(entryA.point, entryB.point));

        if (clusteredEntries.length <= 1) {
            return {
                useCluster: false,
                title: "",
                meta: "",
                groups: [],
                hiddenCount: 0,
            };
        }

        const maxItems = getNearbyPopupMaxItems();
        const visibleEntries = clusteredEntries.slice(0, maxItems);
        const hiddenCount = Math.max(0, clusteredEntries.length - visibleEntries.length);
        const groupsByKey = new Map();

        visibleEntries.forEach((entry) => {
            const descriptor = getClusterGroupDescriptor(entry.point);
            const existingGroup = groupsByKey.get(descriptor.key);

            if (existingGroup) {
                existingGroup.points.push(entry.point);
                existingGroup.distance = Math.min(existingGroup.distance, entry.distance);
                existingGroup.isActiveGroup = existingGroup.isActiveGroup || entry.marker === activeMarker;
                return;
            }

            groupsByKey.set(descriptor.key, {
                key: descriptor.key,
                label: descriptor.label,
                context: descriptor.context,
                stateLabel: descriptor.stateLabel,
                localityLabel: descriptor.localityLabel,
                distance: entry.distance,
                isActiveGroup: entry.marker === activeMarker,
                points: [entry.point],
            });
        });

        const groups = [...groupsByKey.values()]
            .map((group) => ({
                ...group,
                points: [...group.points].sort(compareClusterPointPriority),
            }))
            .sort((groupA, groupB) => {
                const stateDiff = compareDisplayText(groupA.stateLabel, groupB.stateLabel);
                if (stateDiff !== 0) {
                    return stateDiff;
                }

                const localityDiff = compareDisplayText(groupA.localityLabel, groupB.localityLabel);
                if (localityDiff !== 0) {
                    return localityDiff;
                }

                return compareDisplayText(groupA.label, groupB.label);
            });

        const pointCount = clusteredEntries.length;
        const localityCount = groups.length;
        const title = localityCount === 1
            ? groups[0].label
            : `${pointCount} pontos nesta região`;
        const subtitle = localityCount === 1
            ? groups[0].context
            : "";
        const meta = localityCount === 1
            ? `${pointCount} ponto(s) agrupado(s) neste zoom`
            : `${localityCount} localidades agrupadas neste zoom`;

        return {
            useCluster: true,
            title,
            subtitle,
            meta,
            groups,
            hiddenCount,
        };
    }

    /**
     * Keep popup clutter under control by allowing only one popup at a time.
     *
     * Popup cards carry actions and lazy-loading states, so multiple open
     * cards quickly pollute the map during hover exploration.
     */
    function closeOtherPopups(activeMarker) {
        if (panelActiveMarker && panelActiveMarker !== activeMarker) {
            hideHoverPanel();
        }
    }

    /**
     * Explain whether the popup entry refers to the latest known locality or a
     * historical one for the same station.
     */
    function getStationLocationRoleLabel(station) {
        if (station.is_current_location) {
            return "";
        }

        return "Histórico";
    }

    /**
     * Render the station-action entries for one point.
     */
    function buildPointStationEntriesHtml(point) {
        if (!Array.isArray(point.stations) || point.stations.length === 0) {
            return "";
        }

        const orderedStations = [...point.stations].sort(comparePopupStationOrder);
        const isSingleStationPoint = orderedStations.length === 1;

        return orderedStations.map((station) => {
            const equipmentName = escapeHtml(station.equipment_name || "Equipamento");
            const roleLabel = isSingleStationPoint ? "" : getStationLocationRoleLabel(station);
            const hostHref = station.host_id ? `/host?host_id=${station.host_id}&online_only=0` : null;
            const hostSearchHref = !station.host_id && station.equipment_name
                ? `/host?search=${encodeURIComponent(station.equipment_name)}&online_only=0`
                : null;
            const taskHref = station.host_id ? `/task/?host_id=${station.host_id}&online_only=0` : null;
            const spectrumHref = buildSpectrumHref(point, station);

            return `
                <div class="station-entry${isSingleStationPoint ? " station-entry-single" : ""}">
                    <div class="station-entry-header${isSingleStationPoint ? " station-entry-header-single" : ""}">
                        <span class="station-entry-name">${equipmentName}</span>
                        <span class="station-status ${stationStatusClass(station)}">${stationStatusLabel(station)}</span>
                    </div>
                    ${roleLabel ? `<div class="station-entry-context">${escapeHtml(roleLabel)}</div>` : ""}
                    <div class="station-actions">
                        ${spectrumHref ? `<a class="station-action" href="${spectrumHref}" data-loading-message="Abrindo consulta de arquivos...">Arquivos</a>` : ""}
                        ${hostHref ? `<a class="station-action" href="${hostHref}" data-loading-message="Carregando panorama da estação...">Host</a>` : ""}
                        ${hostSearchHref ? `<a class="station-action" href="${hostSearchHref}" data-loading-message="Abrindo consulta de host...">Buscar Host</a>` : ""}
                        ${taskHref ? `<a class="station-action" href="${taskHref}" data-loading-message="Abrindo criação de task...">Criar Task</a>` : ""}
                    </div>
                </div>
            `;
        }).join("");
    }

    /**
     * Render the action area for one point in either detailed or clustered mode.
     */
    function buildPointActionAreaHtml(point, options = {}) {
        const isCluster = Boolean(options.cluster);
        const messageClass = isCluster
            ? "station-popup-cluster-point-message"
            : "station-popup-meta";

        if (point.loadingDetails || (!point.detailsLoaded && !point.detailsError)) {
            return `<div class="${messageClass}">Carregando ações da estação...</div>`;
        }

        if (point.detailsError) {
            return `<div class="${messageClass}">Não foi possível carregar os detalhes desta estação.</div>`;
        }

        if (!Array.isArray(point.stations) || point.stations.length === 0) {
            return `<div class="${messageClass}">Sem equipamento/host vinculado para ações rápidas.</div>`;
        }

        const entriesHtml = buildPointStationEntriesHtml(point);

        if (isCluster) {
            return `
                ${Array.isArray(point.stations) && point.stations.length > 1 ? `<div class="station-popup-cluster-point-section-label">Estações neste ponto</div>` : ""}
                <div class="station-popup-cluster-point-details">
                    ${entriesHtml}
                </div>
            `;
        }

        return `
            <div class="station-popup-section station-popup-primary">
                ${entriesHtml}
            </div>
        `;
    }

    /**
     * Render one point card using the same visual language in both popup modes.
     */
    function buildPointCardHtml(point, options = {}) {
        const stateKey = getPointStateKey(point);
        const pointScopeLabel = options.pointScopeLabel || "";
        const stationCount = Array.isArray(point.stations)
            ? point.stations.length
            : null;
        const stationCountLabel = stationCount === null
            ? null
            : formatStationCountLabel(stationCount);
        const metaParts = [
            `ID_SITE ${point.site_id}`,
        ].filter(Boolean);
        const pointActionsHtml = buildPointActionAreaHtml(point, { cluster: true });
        const pointName = getPointDisplayName(point) || `ID_SITE ${point.site_id}`;
        const locationModeLabel = getPointLocationModeLabel(point);

        return `
            <div class="station-popup-cluster-point">
                ${pointScopeLabel ? `<div class="station-popup-cluster-point-kind">${escapeHtml(pointScopeLabel)}</div>` : ""}
                <div class="station-popup-cluster-point-header">
                    <div class="station-popup-cluster-point-heading">
                        <span class="station-popup-cluster-point-name">${escapeHtml(pointName)}</span>
                    </div>
                    <div class="station-popup-cluster-point-badges">
                        ${stationCountLabel ? `<span class="station-popup-cluster-point-count">${escapeHtml(stationCountLabel)}</span>` : ""}
                        ${locationModeLabel ? `<span class="station-popup-cluster-point-state state-${escapeHtml(stateKey)}">${escapeHtml(locationModeLabel)}</span>` : ""}
                    </div>
                </div>
                <div class="station-popup-cluster-point-meta">${escapeHtml(metaParts.join(" · "))}</div>
                ${pointActionsHtml}
            </div>
        `;
    }

    /**
     * Render the summary popup used when several points collapse together at
     * the current zoom level.
     */
    function buildClusterPopupHtml(clusterSummary) {
        const showGroupHeaders = clusterSummary.groups.length > 1;
        const groupsHtml = clusterSummary.groups.map((group) => {
            const pointRowsHtml = group.points.map((point) => {
                const pointScopeLabel = group.points.length > 1
                    ? "Ponto geográfico"
                    : "";
                return buildPointCardHtml(point, { pointScopeLabel });
            }).join("");

            if (!showGroupHeaders) {
                return pointRowsHtml;
            }

            return `
                <div class="station-popup-cluster-group">
                    <div class="station-popup-cluster-group-header">
                        <span class="station-popup-cluster-group-title">${escapeHtml(group.label)}</span>
                        <span class="station-popup-cluster-group-count">${escapeHtml(`${group.points.length} ponto(s)`)}</span>
                    </div>
                    ${group.context ? `<div class="station-popup-cluster-group-context">${escapeHtml(group.context)}</div>` : ""}
                    <div class="station-popup-cluster-point-list">
                        ${pointRowsHtml}
                    </div>
                </div>
            `;
        }).join("");

        const overflowHtml = clusterSummary.hiddenCount > 0
            ? `<div class="station-popup-cluster-hint">+${clusterSummary.hiddenCount} ponto(s) adicional(is) neste zoom.</div>`
            : "";

        return `
            <div class="station-popup station-popup-cluster">
                <div class="station-popup-title">${escapeHtml(clusterSummary.title)}</div>
                ${clusterSummary.subtitle ? `<div class="station-popup-subtitle">${escapeHtml(clusterSummary.subtitle)}</div>` : ""}
                <div class="station-popup-meta">${escapeHtml(clusterSummary.meta)}</div>
                <div class="station-popup-cluster-groups">
                    ${groupsHtml}
                </div>
                ${overflowHtml}
            </div>
        `;
    }

    // ---------------------------------------------------------------------
    // Legend and popup rendering
    // ---------------------------------------------------------------------
    /**
     * Render only the legend entries that are actually visible in the current
     * filtered point set.
     *
     * The legend is intentionally dynamic so rare states do not create visual
     * noise when they are absent from the current viewport/filter result.
     */
    function renderLegend(points) {
        // The legend is dynamic on purpose: only show states that are actually
        // present in the current filtered view to reduce noise.
        const visibleStates = POINT_STATE_ORDER.filter((stateKey) =>
            points.some((point) => (point.marker_state || "no_host") === stateKey)
        );

        if (!visibleStates.length) {
            legendContainer.hidden = true;
            legendContainer.innerHTML = "";
            return;
        }

        legendContainer.hidden = false;
        legendContainer.innerHTML = visibleStates.map((stateKey) => {
            const meta = POINT_STATE_META[stateKey] || POINT_STATE_META.no_host;
            const historicalClass = isHistoricalMarkerState(stateKey) ? " is-historical" : "";
            return `
                <span class="legend-item">
                    <span
                        class="legend-swatch${historicalClass}"
                        style="--marker-fill: ${meta.color}; --marker-stripe-color: ${meta.stripeColor || "rgba(255, 255, 255, 0.4)"};"
                    ></span>
                    ${escapeHtml(meta.legendLabel)}
                </span>
            `;
        }).join("");
    }

    /**
     * Build the popup markup for one locality marker.
     *
     * Popups have three states:
     *   - loading skeleton while the lazy detail request is in flight
     *   - graceful error fallback when detail fetch fails
     *   - full station action list once detail data is available
     */
    function buildPopupHtml(point) {
        if (point.popupClusterSummary?.useCluster) {
            return buildClusterPopupHtml(point.popupClusterSummary);
        }

        // Popups have three rendering phases:
        //   1. skeleton/loading
        //   2. graceful degradation on detail error
        //   3. full action set once lazy details arrive
        //
        // That staged rendering is intentional: hovering a point should feel
        // immediate even before the detailed station payload arrives, but the
        // popup must still degrade cleanly when the detail request fails.
        const stateLabel = getPointStateDisplayLabel(point);
        const localityLabel = getPointLocalityShortLabel(point) || getPointLocalityLabel(point);
        const pointCardHtml = buildPointCardHtml(point);

        return `
            <div class="station-popup">
                <div class="station-popup-title">${escapeHtml(stateLabel)}</div>
                ${localityLabel ? `<div class="station-popup-subtitle">${escapeHtml(localityLabel)}</div>` : ""}
                <div class="station-popup-cluster-point-list station-popup-single-point-list">
                    ${pointCardHtml}
                </div>
            </div>
        `;
    }

    /**
     * Repaint a marker after lazy point details refine its marker state.
     *
     * The initial `/api/map/stations` snapshot is already useful, but the
     * per-site detail response is authoritative for popup-level actions and
     * can also refine the visual state.
     */
    function updateMarkerAppearance(marker, point) {
        /* Lazy detail responses can refine both popup content and marker
         * state. Updating the existing icon in place preserves the current
         * hover/popup context instead of tearing the marker down.
         */
        if (typeof marker.setIcon === "function") {
            marker.setIcon(createMarkerIcon(point));
            ensureMarkerElementInteractivity(marker);
        }
    }

    /**
     * Resize already-rendered markers after zoom changes.
     *
     * We update in place instead of fully re-rendering the layer so the map
     * stays responsive and any currently open popup survives the zoom.
     */
    function refreshRenderedMarkerAppearance() {
        renderedMarkers.forEach((marker) => {
            if (!marker.__wfPoint) {
                return;
            }

            updateMarkerAppearance(marker, marker.__wfPoint);
        });
    }

    // Hover popups stay lightweight at first render; station actions are
    // loaded lazily only when the user actually opens a point.

    /**
     * Compute the pixel-space position for the hover panel relative to the
     * `.station-map-wrap` container, clamped so the panel never overflows
     * the map edges.
     */
    function positionHoverPanel(marker) {
        if (!marker || hoverPanel.hidden) {
            return;
        }

        // Use getBoundingClientRect so the coordinates are in the same reference
        // frame as the panel's offset parent, regardless of browser zoom level or
        // any CSS transform on the map container.
        const markerEl = marker.getElement();
        if (!markerEl) {
            return;
        }

        const wrapEl     = hoverPanel.parentElement;
        const markerRect = markerEl.getBoundingClientRect();
        const wrapRect   = wrapEl.getBoundingClientRect();

        const panelW = hoverPanel.offsetWidth  || 340;
        const panelH = hoverPanel.offsetHeight || 200;
        const gap     = 10;
        const padding = 8;

        // Marker centre relative to the wrap element (the panel's offset parent).
        const markerCX    = markerRect.left - wrapRect.left + markerRect.width  / 2;
        const markerCY    = markerRect.top  - wrapRect.top  + markerRect.height / 2;
        const halfMarkerH = markerRect.height / 2;

        const wrapW = wrapRect.width;
        const wrapH = wrapRect.height;
        const viewportH = window.innerHeight || document.documentElement.clientHeight || wrapH;
        const visibleTopInWrap = Math.max(padding, (0 - wrapRect.top) + padding);
        const visibleBottomInWrap = Math.min(
            wrapH - padding,
            viewportH - wrapRect.top - padding
        );
        const availablePanelHeight = Math.max(
            180,
            Math.floor(visibleBottomInWrap - visibleTopInWrap)
        );

        hoverPanel.style.maxHeight = `${availablePanelHeight}px`;

        const measuredPanelH = hoverPanel.offsetHeight || panelH;

        // Default: centre horizontally on the marker, open above it.
        let left = markerCX - panelW / 2;
        let top  = markerCY - halfMarkerH - gap - measuredPanelH;

        // Horizontal clamp.
        if (left < padding)                  left = padding;
        if (left + panelW > wrapW - padding) left = wrapW - padding - panelW;

        // Prefer above; flip below when the panel would overflow the top edge.
        if (top < visibleTopInWrap) {
            top = markerCY + halfMarkerH + gap;
        }

        // Bottom clamp — catches both the default position and the flipped one.
        if (top + measuredPanelH > visibleBottomInWrap) {
            top = visibleBottomInWrap - measuredPanelH;
        }

        // Final safety: keep the panel inside the visible browser area even
        // when the map is taller than the viewport.
        if (top < visibleTopInWrap) top = visibleTopInWrap;

        hoverPanel.style.left = Math.round(left) + "px";
        hoverPanel.style.top  = Math.round(top)  + "px";
    }

    /**
     * Display the hover panel for `marker`, load lazy details, and close any
     * other panel that may have been open.
     */
    function showHoverPanel(marker) {
        const point = marker?.__wfPoint;
        if (!point) {
            return;
        }

        closeOtherPopups(marker);
        panelActiveMarker = marker;

        hoverPanel.innerHTML = buildPopupHtml(point);
        hoverPanel.removeAttribute("hidden");

        // Position immediately, then again after the first paint so the panel
        // height is known and the vertical clamp is accurate.
        positionHoverPanel(marker);
        window.requestAnimationFrame(() => {
            if (panelActiveMarker === marker) positionHoverPanel(marker);
        });

        refreshPopupNearbySummary(marker);
        if (point.popupClusterSummary?.useCluster) {
            ensureClusterPointDetails(marker);
        } else {
            ensurePointDetails(point, marker);
        }
    }

    /**
     * Hide the hover panel and clear the active-marker reference.
     */
    function hideHoverPanel() {
        panelActiveMarker = null;
        hoverPanel.setAttribute("hidden", "");
        hoverPanel.innerHTML = "";
    }

    /**
     * Refresh the nearby-point summary attached to one popup marker.
     */
    function refreshPopupNearbySummary(marker) {
        const point = marker?.__wfPoint;

        if (!point) {
            return;
        }

        const nearbySummary = buildNearbyPopupSummary(marker);
        point.popupClusterSummary = nearbySummary.useCluster ? nearbySummary : null;

        if (panelActiveMarker === marker) {
            hoverPanel.innerHTML = buildPopupHtml(point);
            // Re-clamp after a content change because the panel height may differ.
            window.requestAnimationFrame(() => {
                if (panelActiveMarker === marker) positionHoverPanel(marker);
            });
        }
    }

    /**
     * Lazy-load details for every point currently listed inside a cluster popup.
     */
    function ensureClusterPointDetails(marker) {
        const clusterSummary = marker?.__wfPoint?.popupClusterSummary;

        if (!clusterSummary?.useCluster) {
            return;
        }

        const seenPoints = new Set();
        clusterSummary.groups.forEach((group) => {
            group.points.forEach((point) => {
                if (seenPoints.has(point)) {
                    return;
                }

                seenPoints.add(point);
                ensurePointDetails(point, marker);
            });
        });
    }

    /**
     * Refresh the panel content after a zoom change that may alter the nearby
     * cluster summary.
     */
    function refreshOpenPopupNearbyPoints() {
        if (!panelActiveMarker) {
            return;
        }

        const marker = panelActiveMarker;
        refreshPopupNearbySummary(marker);

        if (marker.__wfPoint?.popupClusterSummary?.useCluster) {
            ensureClusterPointDetails(marker);
        } else {
            ensurePointDetails(marker.__wfPoint, marker);
        }
    }

    /**
     * Fetch and cache popup details for one site exactly once.
     *
     * The home page first paints from a summary payload. Detailed station
     * actions are lazy-loaded on hover to keep the initial page load light.
     */
    function loadPointDetails(point, popupMarker) {
        // Cache per point so repeated hover/open interactions do not keep
        // hammering `/api/map/stations/<site_id>`.
        //
        // The summary payload gives enough data for first paint and filtering.
        // The detail payload is reserved for richer popup actions and, when
        // needed, a more authoritative marker state. That separation is one of
        // the reasons the home page still feels light on cold loads.
        if (point.detailsLoaded) {
            return Promise.resolve(point);
        }

        if (point.loadingPromise) {
            return point.loadingPromise;
        }

        point.loadingDetails = true;
        if (popupMarker) {
            refreshPopupNearbySummary(popupMarker);
        }

        point.loadingPromise = fetch(buildMapApiUrl(`/api/map/stations/${point.site_id}`))
            .then((response) => response.json())
            .then((payload) => {
                let detailStations = Array.isArray(payload.stations) ? payload.stations : [];
                const activeStationScope = point._activeStationScope
                    ? buildActiveStationScope(point._activeStationScope)
                    : buildActiveStationScope({
                        searchTerm: normalizeSearchText(point._stationScopeSearchTerm || ""),
                    });

                if (hasActiveStationScope(activeStationScope)) {
                    detailStations = filterStationsByScope(detailStations, activeStationScope);
                }

                point.loadingDetails = false;
                point.detailsLoaded = true;
                point.detailsError = false;
                point.stations = detailStations;
                point.marker_state = hasActiveStationScope(activeStationScope)
                    ? summarizePointStateFromStations(detailStations)
                    : payload.marker_state || point.marker_state || "no_host";
                point.has_online_station = detailStations.some((station) =>
                    ONLINE_POINT_STATES.has(station?.map_state || "no_host")
                );
                point.has_online_host = point.has_online_station;
                point.has_known_host = detailStations.some((station) =>
                    station?.host_id !== null && station?.host_id !== undefined
                );
                point.station_names = point.stations
                    .map((station) => station.host_name || station.equipment_name || "")
                    .filter(Boolean);
                updateMarkerAppearance(point._marker || popupMarker, point);
                if (popupMarker) {
                    refreshPopupNearbySummary(popupMarker);
                }
                return point;
            })
            .catch(() => {
                point.loadingDetails = false;
                point.detailsError = true;
                if (popupMarker) {
                    refreshPopupNearbySummary(popupMarker);
                }
                return point;
            })
            .finally(() => {
                point.loadingPromise = null;
            });

        return point.loadingPromise;
    }

    /**
     * Start the lazy detail request only when the point is still unresolved.
     *
     * Both `mouseover` and `popupopen` can hit this path, so this guard keeps
     * those events from duplicating network work.
     */
    function ensurePointDetails(point, marker) {
        if (point.detailsLoaded || point.loadingDetails) {
            return;
        }

        loadPointDetails(point, marker);
    }

    /**
     * Create one Leaflet marker, its popup, hover behavior and bounds
     * contribution for a filtered point.
     *
     * Markers are intentionally rebuilt after each filter change because the
     * point set is still manageable and the simpler lifecycle has been more
     * reliable than trying to diff/update marker instances in place.
     */
    function addPointToMap(point) {
        // Markers are rebuilt from scratch on every filter change. That is
        // acceptable here because the filtered point set is still manageable
        // and it keeps the rendering logic simple and predictable.
        //
        // In exchange for that simpler lifecycle, this function becomes the
        // single source of truth for marker wiring: popup, hover delay, lazy
        // detail fetch, nearby-point helper and bounds contribution all live
        // here instead of being spread across several render/update phases.
        if (typeof point.latitude !== "number" || typeof point.longitude !== "number") {
            return;
        }

        const marker = createLeafletMarker(point).addTo(markerLayer);
        ensureMarkerElementInteractivity(marker);

        point._marker = marker;
        marker.__wfPoint = point;
        renderedMarkers.push(marker);

        let openTimer = null;
        let closeTimer = null;

        function cancelOpen() {
            if (openTimer) {
                window.clearTimeout(openTimer);
                openTimer = null;
            }
        }

        // Hovering from marker to popup briefly leaves the marker hitbox. A
        // short delayed close keeps that journey feeling forgiving instead of
        // collapsing the popup the moment the cursor crosses the gap.
        function cancelClose() {
            if (closeTimer) {
                window.clearTimeout(closeTimer);
                closeTimer = null;
            }
        }

        function scheduleOpen() {
            /* Hover popups use a small delay to distinguish intentional
             * inspection from the cursor merely sweeping across the map. This
             * reduces accidental popup spam and avoids eager detail loading for
             * points the operator never truly stopped on.
             */
            cancelOpen();
            openTimer = window.setTimeout(() => {
                openTimer = null;
                showHoverPanel(marker);
            }, POPUP_HOVER_OPEN_DELAY_MS);
        }

        function scheduleClose() {
            cancelOpen();
            cancelClose();
            closeTimer = window.setTimeout(hideHoverPanel, 180);
        }

        // Store references so the panel's own hover listeners can cancel/resume
        // the close timer when the cursor travels from marker to panel.
        marker.__wfCancelClose  = cancelClose;
        marker.__wfScheduleClose = scheduleClose;

        marker.on("mouseover", function () {
            cancelClose();

            if (typeof marker.bringToFront === "function") {
                marker.bringToFront();
            }

            scheduleOpen();
        });

        marker.on("mouseout", scheduleClose);

        bounds.push([point.latitude, point.longitude]);
    }

    // ---------------------------------------------------------------------
    // Filter and combobox model
    // ---------------------------------------------------------------------
    /**
     * Sort site options by display label, using `site_id` as a stable tie
     * breaker so the combobox order does not flicker between renders.
     */
    function sortSites(points) {
        return [...points].sort((a, b) => {
            const labelA = String(getPointDisplayName(a) || "").toLowerCase();
            const labelB = String(getPointDisplayName(b) || "").toLowerCase();

            if (labelA === labelB) {
                return a.site_id - b.site_id;
            }

            return labelA.localeCompare(labelB, "pt-BR");
        });
    }

    /**
     * Rebuild the state selector from the currently loaded map dataset.
     *
     * The control is data-driven rather than hardcoded so it reflects exactly
     * the federative units present in the current backend snapshot.
     */
    function populateStateFilter(points) {
        /* The state filter is derived from the current dataset rather than
         * hardcoded. That keeps the control honest: it only offers federative
         * units that are actually represented in the loaded point snapshot.
         */
        const states = [...new Map(
            points
                .filter((point) => point.state_code)
                .map((point) => [
                    point.state_code,
                    {
                        code: point.state_code,
                        name: point.state_name || point.state_code
                    }
                ])
        ).values()].sort((a, b) => a.code.localeCompare(b.code, "pt-BR"));

        stateFilter.innerHTML = '<option value="">Todos os estados</option>';

        states.forEach((state) => {
            const option = document.createElement("option");
            option.value = state.code;
            option.textContent = `${state.code} - ${state.name}`;
            stateFilter.appendChild(option);
        });
    }

    /**
     * Rebuild the station/site combobox options, optionally constrained by the
     * currently selected state.
     *
     * Duplicate human labels are disambiguated here so later rendering and
     * lookup logic can stay simple.
     */
    function populateSiteFilter(points, selectedStateCode) {
        // Changing the state rebuilds the candidate list so the combobox never
        // offers stations that are impossible under the current geography.
        const filteredByState = selectedStateCode
            ? points.filter((point) => point.state_code === selectedStateCode)
            : points;
        const sortedPoints = sortSites(filteredByState);
        const baseLabelCounts = new Map();
        const aliasCounts = new Map();

        sortedPoints.forEach((point) => {
            const baseLabel = getPointOptionBaseLabel(point);
            baseLabelCounts.set(baseLabel, (baseLabelCounts.get(baseLabel) || 0) + 1);
            getPointSearchAliases(point).forEach((alias) => {
                const aliasKey = normalizeSearchText(alias);
                aliasCounts.set(aliasKey, (aliasCounts.get(aliasKey) || 0) + 1);
            });
        });

        siteOptionRecords = [];
        siteOptionIndex = new Map();
        siteOptionRecords.push({site_id: "", label: ALL_SITES_LABEL});

        sortedPoints.forEach((point) => {
            const baseLabel = getPointOptionBaseLabel(point);
            const optionLabel = baseLabelCounts.get(baseLabel) > 1
                ? `${baseLabel} · ${getPointOptionDisambiguator(point)}`
                : baseLabel;
            siteOptionRecords.push({
                site_id: String(point.site_id),
                label: optionLabel,
                point
            });
            siteOptionIndex.set(normalizeSearchText(optionLabel), String(point.site_id));

            getPointSearchAliases(point).forEach((alias) => {
                const aliasLabel = buildStationMatchLabel(point, alias);
                siteOptionIndex.set(normalizeSearchText(aliasLabel), String(point.site_id));

                const aliasKey = normalizeSearchText(alias);
                if ((aliasCounts.get(aliasKey) || 0) === 1) {
                    siteOptionIndex.set(aliasKey, String(point.site_id));
                }
            });
        });

        if (selectedSiteId) {
            const selectedOption = siteOptionRecords.find((option) => option.site_id === String(selectedSiteId));

            if (selectedOption) {
                siteFilter.value = selectedOption.label;
            } else {
                selectedSiteId = "";
            }
        }

        renderSiteMenu();
    }

    /**
     * Render the visible combobox option menu from the current search term.
     *
     * The menu is capped intentionally to keep DOM churn bounded even when the
     * overall point list is large.
     */
    function renderSiteMenu() {
        if (!siteMenu) {
            return;
        }

        // Keep the synthetic "all stations" option always present so the user
        // can get back to the unfiltered state even while typing.
        const searchTerm = normalizeSearchText(siteFilter.value);
        const stationMatchOptions = getStationMatchOptions(searchTerm);
        const visibleOptions = (
            stationMatchOptions.length > 0
                ? [siteOptionRecords[0], ...stationMatchOptions]
                : siteOptionRecords.filter((option) => {
                    if (!option.site_id) {
                        return true;
                    }

                    if (!searchTerm) {
                        return true;
                    }

                    return normalizeSearchText(option.label).includes(searchTerm);
                })
        ).slice(0, 120);

        if (visibleOptions.length === 0) {
            siteMenu.innerHTML = '<div class="station-map-combobox-empty">Nenhuma estação encontrada para esse filtro.</div>';
            return;
        }

        siteMenu.innerHTML = visibleOptions.map((option) => {
            const isActive = option.site_id && option.site_id === String(selectedSiteId);
            return `
                <button
                    type="button"
                    class="station-map-combobox-option${isActive ? " is-active" : ""}"
                    data-site-option="${option.site_id}"
                >
                    ${escapeHtml(option.label)}
                </button>
            `;
        }).join("");
    }

    /**
     * Open the station combobox dropdown with freshly filtered options.
     *
     * Re-rendering on open keeps the menu aligned with the current state
     * filter and the latest free-text typed into the combobox input.
     */
    function openSiteMenu() {
        renderSiteMenu();
        siteMenu.hidden = false;
    }

    /**
     * Hide the station combobox dropdown.
     *
     * Hiding the menu is intentionally non-destructive: it does not clear the
     * typed search text nor the current exact selection.
     */
    function closeSiteMenu() {
        siteMenu.hidden = true;
    }

    /**
     * Commit one explicit site selection from the combobox UI and refresh the
     * filtered map in one place.
     *
     * This helper is the only path that upgrades a human-facing label into the
     * exact `site_id` understood by the map filters and zoom heuristics.
     */
    function applySiteSelection(siteId, label) {
        // This is the only path that upgrades a human-facing label into the
        // exact `site_id` used by the map filters.
        selectedSiteId = String(siteId || "");
        siteFilter.value = selectedSiteId ? label : "";
        closeSiteMenu();
        renderFilteredPoints();
    }

    /**
     * Apply the current state/site/search/scope controls to the full
     * point dataset and return the subset that should be rendered.
     *
     * Filter order mirrors the user's mental model:
     *   1. geographic narrowing by state
     *   2. exact selected site, when present
     *   3. free-text search fallback
     *   4. status scope
     *   5. locality scope
     */
    function getFilteredPoints() {
        const selectedStateCode = stateFilter.value;
        const activeSiteId = getSelectedSiteId();
        const siteSearchTerm = activeSiteId
            ? ""
            : normalizeSearchText(siteFilter.value) === normalizeSearchText(ALL_SITES_LABEL)
            ? ""
            : normalizeSearchText(siteFilter.value);
        const statusScope = statusFilter ? statusFilter.value : "all";
        const localityScope = localityFilter ? localityFilter.value : "include_history";
        const activeStationScope = buildActiveStationScope({
            searchTerm: siteSearchTerm,
            statusScope,
            localityScope,
        });
        const hasStationScope = hasActiveStationScope(activeStationScope);

        return allStationPoints.reduce((filteredPoints, point) => {
            if (selectedStateCode && point.state_code !== selectedStateCode) {
                return filteredPoints;
            }

            if (activeSiteId) {
                if (String(point.site_id) !== activeSiteId) {
                    return filteredPoints;
                }
            } else if (siteSearchTerm && !getPointSearchText(point).includes(siteSearchTerm)) {
                return filteredPoints;
            }

            let effectivePoint = point;

            if (hasStationScope) {
                if (!Array.isArray(point.stations) || point.stations.length === 0) {
                    return filteredPoints;
                }

                const matchedStations = filterStationsByScope(point.stations, activeStationScope);

                if (matchedStations.length === 0) {
                    return filteredPoints;
                }

                effectivePoint = buildStationScopedPoint(point, matchedStations, activeStationScope);
            }

            if (statusScope === "online_only" && !isOnlinePoint(effectivePoint)) {
                return filteredPoints;
            }

            if (statusScope === "offline_only" && !isOfflinePoint(effectivePoint)) {
                return filteredPoints;
            }

            if (localityScope === "current_only" && isHistoricalPoint(effectivePoint)) {
                return filteredPoints;
            }

            if (localityScope === "historical_only" && !isHistoricalPoint(effectivePoint)) {
                return filteredPoints;
            }

            filteredPoints.push(effectivePoint);
            return filteredPoints;
        }, []);
    }

    /**
     * Re-render the markers, legend and map bounds from the current control
     * state.
     *
     * This is the single visual refresh entrypoint used by every control so
     * the map, point count, legend and zoom behavior stay consistent.
     */
    function renderFilteredPoints() {
        // Every toolbar change funnels through here so markers, legend, count
        // and zoom always reflect the same filtered dataset.
        const filteredPoints = getFilteredPoints();
        const orderedPoints = [...filteredPoints].sort((pointA, pointB) => {
            const priorityDiff = getPointRenderPriority(pointA) - getPointRenderPriority(pointB);

            if (priorityDiff !== 0) {
                return priorityDiff;
            }

            return (pointA.site_id || 0) - (pointB.site_id || 0);
        });
        markerLayer.clearLayers();
        bounds.length = 0;
        renderedMarkers = [];

        orderedPoints.forEach((point) => addPointToMap(point));
        renderLegend(filteredPoints);

        pointCount.textContent = `${filteredPoints.length} ponto(s) plotado(s)`;
        updateClearFiltersButtonState();

        if (filteredPoints.length > 0 && bounds.length > 0) {
            map.fitBounds(bounds, {
                padding: [30, 30],
                maxZoom: getSelectedSiteId() ? 11 : 8
            });
        } else {
            map.setView(defaultCenter, 4);
        }
    }

    /**
     * Reload the summary-backed point dataset using the current temporal range.
     *
     * State/site/status/locality controls continue to run client-side on top of
     * this dataset, but the time window is applied server-side so the marker
     * universe itself matches the requested observation interval.
     */
    function loadStationPoints() {
        const requestId = ++latestDatasetRequestId;
        const previousStateCode = stateFilter ? stateFilter.value : "";
        const previousSiteSearch = siteFilter ? siteFilter.value : "";
        const previousSelectedSiteId = selectedSiteId;

        pointCount.textContent = "Carregando pontos...";

        return fetch(buildMapApiUrl("/api/map/stations"))
            .then((response) => response.json())
            .then((payload) => {
                if (requestId !== latestDatasetRequestId) {
                    return;
                }

                const stationPoints = Array.isArray(payload.points) ? payload.points : [];
                allStationPoints = stationPoints;
                populateStateFilter(allStationPoints);

                if (
                    previousStateCode
                    && Array.from(stateFilter.options).some((option) => option.value === previousStateCode)
                ) {
                    stateFilter.value = previousStateCode;
                } else {
                    stateFilter.value = "";
                }

                selectedSiteId = previousSelectedSiteId;
                populateSiteFilter(allStationPoints, stateFilter.value);

                if (!selectedSiteId && siteFilter) {
                    siteFilter.value = previousSiteSearch;
                }

                renderFilteredPoints();
            })
            .catch(() => {
                if (requestId !== latestDatasetRequestId) {
                    return;
                }

                allStationPoints = [];
                selectedSiteId = "";
                markerLayer.clearLayers();
                renderedMarkers = [];
                bounds.length = 0;
                populateStateFilter(allStationPoints);
                populateSiteFilter(allStationPoints, "");
                renderLegend([]);
                pointCount.textContent = "Mapa em modo degradado";
                updateClearFiltersButtonState();
            });
    }

    // Changing the state invalidates the previous site choice because the site
    // list is rebuilt from the state-scoped subset of points.
    stateFilter.addEventListener("change", () => {
        selectedSiteId = "";
        siteFilter.value = "";
        populateSiteFilter(allStationPoints, stateFilter.value);
        renderFilteredPoints();
    });

    siteFilter.addEventListener("focus", () => {
        openSiteMenu();
    });

    // `input` keeps typing reactive: it updates the free-text filtering path
    // and also captures exact label matches back into `selectedSiteId`.
    siteFilter.addEventListener("input", () => {
        selectedSiteId = siteOptionIndex.get(normalizeSearchText(siteFilter.value)) || "";
        openSiteMenu();
        renderFilteredPoints();
    });

    // `change` normalizes blur/commit behavior. Only an exact label match is
    // promoted to a real site selection; otherwise the field stays a search.
    siteFilter.addEventListener("change", () => {
        const normalizedValue = normalizeSearchText(siteFilter.value);

        if (!normalizedValue || normalizedValue === normalizeSearchText(ALL_SITES_LABEL)) {
            selectedSiteId = "";
            siteFilter.value = "";
        } else if (siteOptionIndex.has(normalizedValue)) {
            selectedSiteId = siteOptionIndex.get(normalizedValue) || "";
        }

        closeSiteMenu();
        renderFilteredPoints();
    });

    // Escape dismisses the menu. Enter commits the current typed value only
    // when it exactly matches a known option label.
    siteFilter.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeSiteMenu();
        }

        if (event.key === "Enter") {
            const normalizedValue = normalizeSearchText(siteFilter.value);
            const matchedSiteId = siteOptionIndex.get(normalizedValue) || "";

            if (matchedSiteId) {
                event.preventDefault();
                applySiteSelection(matchedSiteId, siteFilter.value);
            }
        }
    });

    // `mousedown` wins the race against blur/outside-click handlers. Using
    // `click` here would occasionally close the menu before committing the
    // chosen option.
    siteMenu.addEventListener("mousedown", (event) => {
        const button = event.target.closest("[data-site-option]");

        if (!button) {
            return;
        }

        event.preventDefault();
        applySiteSelection(
            button.getAttribute("data-site-option"),
            button.textContent.trim()
        );
    });

    // Clicking outside the combobox shell should dismiss the menu, matching
    // the behavior users expect from a native dropdown.
    document.addEventListener("click", (event) => {
        if (!event.target.closest(".station-map-combobox-wrap")) {
            closeSiteMenu();
        }
    });

    if (clearFiltersButton) {
        clearFiltersButton.addEventListener("mousedown", (event) => {
            event.preventDefault();
            clearMapFilters();
        });

        clearFiltersButton.addEventListener("click", (event) => {
            if (event.detail !== 0) {
                return;
            }

            event.preventDefault();
            clearMapFilters();
        });
    }

    if (statusFilter) {
        statusFilter.addEventListener("change", () => {
            renderFilteredPoints();
        });
    }

    if (localityFilter) {
        localityFilter.addEventListener("change", () => {
            renderFilteredPoints();
        });
    }

    temporalFilterFields.forEach((field) => {
        if (field.textInput) {
            field.textInput.addEventListener("input", () => {
                handleTemporalFieldTyping(field);
            });
            field.textInput.addEventListener("change", () => {
                commitTemporalFilterChange();
            });
            field.textInput.addEventListener("keydown", (event) => {
                if (event.key === "Enter") {
                    event.preventDefault();
                    event.currentTarget.blur();
                }
            });
        }

        if (field.nativeInput) {
            field.nativeInput.addEventListener("change", () => {
                setTemporalFieldIsoValue(field, field.nativeInput.value);
                commitTemporalFilterChange();
            });
        }
    });

    if (themeSelect) {
        // Theme is shell state rather than data state, so it is restored once
        // and then left independent from the filter/render cycle.
        themeSelect.value = loadSavedMapTheme();
        themeSelect.addEventListener("change", () => {
            applyMapTheme(themeSelect.value);
        });
    }

    map.on("zoomend", () => {
        refreshRenderedMarkerAppearance();
        refreshOpenPopupNearbyPoints();
        if (panelActiveMarker) positionHoverPanel(panelActiveMarker);
    });

    // Reposition the panel while the map is panned so it tracks the marker.
    map.on("moveend", () => {
        if (panelActiveMarker) positionHoverPanel(panelActiveMarker);
    });

    window.addEventListener("resize", () => {
        if (panelActiveMarker) positionHoverPanel(panelActiveMarker);
    }, { passive: true });

    window.addEventListener("scroll", () => {
        if (panelActiveMarker) positionHoverPanel(panelActiveMarker);
    }, { passive: true });

    // Startup restores the shell theme first, then normalizes the temporal
    // controls, then loads the initial summary-backed point dataset.
    applyMapTheme(loadSavedMapTheme());
    syncTemporalFields({syncDisplay: true});

    // The first payload is intentionally summary-only. Full station actions are
    // deferred to the lazy popup requests so the home page stays responsive.
    loadStationPoints();
})();
