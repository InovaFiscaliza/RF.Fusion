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
    const stateFilter = document.getElementById("station-map-state-filter");
    const siteFilter = document.getElementById("station-map-site-filter");
    const siteMenu = document.getElementById("station-map-site-menu");
    const themeSelect = document.getElementById("station-map-theme-select");
    const statusFilter = document.getElementById("station-map-status-filter");
    const localityFilter = document.getElementById("station-map-locality-filter");
    const startDateFilter = document.getElementById("station-map-start-date");
    const endDateFilter = document.getElementById("station-map-end-date");
    const legendContainer = document.getElementById("station-map-legend");

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

    const MAP_THEME_STORAGE_KEY = "webfusion.station_map_theme";
    const POPUP_HOVER_OPEN_DELAY_MS = 140;
    const MARKER_FOCUS_ZOOM_THRESHOLD = 7;
    const MAX_NEARBY_POPUP_POINTS = 5;
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
     * Produce the human-facing point label used in tooltips and selectors.
     *
     * When multiple stations share one plotted locality we intentionally
     * collapse the label to `first (+N)` so the map stays scannable.
     */
    function getPointDisplayName(point) {
        const namedStations = Array.isArray(point.stations) && point.stations.length > 0
            ? point.stations
                .map((station) => station.host_name || station.equipment_name)
                .filter(Boolean)
            : Array.isArray(point.station_names)
            ? point.station_names.filter(Boolean)
            : [];

        if (namedStations.length === 1) {
            return namedStations[0];
        }

        if (namedStations.length > 1) {
            return `${namedStations[0]} (+${namedStations.length - 1})`;
        }

        return point.site_label;
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
     * Normalize the optional temporal controls into query parameters.
     *
     * The browser date input already emits `YYYY-MM-DD`, so lexical order is
     * safe when we need to repair an inverted range quickly in the UI.
     */
    function getTemporalFilterQueryParams() {
        let startDate = startDateFilter ? startDateFilter.value : "";
        let endDate = endDateFilter ? endDateFilter.value : "";

        if (startDate && endDate && startDate > endDate) {
            const swappedStart = endDate;
            const swappedEnd = startDate;
            startDate = swappedStart;
            endDate = swappedEnd;

            if (startDateFilter) {
                startDateFilter.value = startDate;
            }

            if (endDateFilter) {
                endDateFilter.value = endDate;
            }
        }

        const params = new URLSearchParams();

        if (startDate) {
            params.set("start_date", startDate);
        }

        if (endDate) {
            params.set("end_date", endDate);
        }

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
            popupAnchor: [0, -metrics.popupOffsetY],
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
        element.addEventListener("click", () => marker.openPopup());
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
        const districtName = String(point?.district_name || "").trim();
        const siteLabel = String(point?.site_label || "").trim();
        const countyName = String(point?.county_name || "").trim();
        const stateCode = String(point?.state_code || "").trim();
        const countyStateLabel = getPointCountyStateLabel(point) || countyName || stateCode;
        const districtKey = normalizeSearchText(districtName);
        const siteKey = normalizeSearchText(siteLabel);
        const countyKey = normalizeSearchText(countyName);
        const stateKey = normalizeSearchText(stateCode);

        if (siteKey && siteKey !== countyKey) {
            return {
                key: `${stateKey}|${countyKey}|site|${siteKey}`,
                label: siteLabel,
                context: countyStateLabel,
            };
        }

        if (districtKey && districtKey !== countyKey) {
            return {
                key: `${stateKey}|${countyKey}|district|${districtKey}`,
                label: districtName,
                context: countyStateLabel,
            };
        }

        return {
            key: `${stateKey}|${countyKey || siteKey || districtKey || "fallback"}`,
            label: countyStateLabel || `ID_SITE ${point.site_id}`,
            context: countyStateLabel,
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
        const priorityDiff = getPointRenderPriority(pointB) - getPointRenderPriority(pointA);

        if (priorityDiff !== 0) {
            return priorityDiff;
        }

        return (pointA.site_id || 0) - (pointB.site_id || 0);
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
            .sort((entryA, entryB) => {
                if (entryA.distance !== entryB.distance) {
                    return entryA.distance - entryB.distance;
                }

                return compareClusterPointPriority(entryA.point, entryB.point);
            });

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
                if (groupA.isActiveGroup !== groupB.isActiveGroup) {
                    return groupA.isActiveGroup ? -1 : 1;
                }

                if (groupA.distance !== groupB.distance) {
                    return groupA.distance - groupB.distance;
                }

                return String(groupA.label || "").localeCompare(String(groupB.label || ""), "pt-BR");
            });

        const pointCount = clusteredEntries.length;
        const localityCount = groups.length;
        const title = localityCount === 1
            ? groups[0].label
            : `${pointCount} pontos nesta região`;
        const meta = localityCount === 1
            ? [groups[0].context, `${pointCount} ponto(s) agrupado(s) neste zoom`].filter(Boolean).join(" · ")
            : `${localityCount} localidades agrupadas neste zoom`;

        return {
            useCluster: true,
            title,
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
        renderedMarkers.forEach((marker) => {
            if (marker !== activeMarker && marker.isPopupOpen()) {
                marker.closePopup();
            }
        });
    }

    /**
     * Explain whether the popup entry refers to the latest known locality or a
     * historical one for the same station.
     */
    function getStationLocationRoleLabel(station) {
        if (station.is_current_location) {
            return "Posição atual";
        }

        return "Localidade histórica";
    }

    /**
     * Compose the compact metadata line shown under a popup title.
     */
    function getPointPopupMeta(point, separator = " | ") {
        return [
            `ID_SITE ${point.site_id}`,
            getPointLocalityLabel(point) ? `Localidade ${getPointLocalityLabel(point)}` : null,
            point.altitude !== null && point.altitude !== undefined ? `Alt ${point.altitude} m` : null,
            point.gnss_measurements ? `${point.gnss_measurements} medições GNSS` : null
        ].filter(Boolean).join(separator);
    }

    /**
     * Render the station-action entries for one point.
     */
    function buildPointStationEntriesHtml(point) {
        if (!Array.isArray(point.stations) || point.stations.length === 0) {
            return "";
        }

        return point.stations.map((station) => {
            const equipmentName = escapeHtml(station.equipment_name || "Equipamento");
            const localityContext = escapeHtml(
                [
                    getPointLocalityLabel(point) || "Localidade não identificada",
                    getStationLocationRoleLabel(station)
                ].join(" · ")
            );
            const hostHref = station.host_id ? `/host?host_id=${station.host_id}&online_only=0` : null;
            const hostSearchHref = !station.host_id && station.equipment_name
                ? `/host?search=${encodeURIComponent(station.equipment_name)}&online_only=0`
                : null;
            const taskHref = station.host_id ? `/task/?host_id=${station.host_id}&online_only=0` : null;
            const spectrumHref = buildSpectrumHref(point, station);

            return `
                <div class="station-entry">
                    <div class="station-entry-header">
                        <span class="station-entry-name">${equipmentName}</span>
                        <span class="station-status ${stationStatusClass(station)}">${stationStatusLabel(station)}</span>
                    </div>
                    <div class="station-entry-context">${localityContext}</div>
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
                <div class="station-popup-cluster-point-section-label">Estações neste ponto</div>
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
     * Render the summary popup used when several points collapse together at
     * the current zoom level.
     */
    function buildClusterPopupHtml(clusterSummary) {
        const groupsHtml = clusterSummary.groups.map((group) => {
            const pointRowsHtml = group.points.map((point) => {
                const stateKey = getPointStateKey(point);
                const stateMeta = POINT_STATE_META[stateKey] || POINT_STATE_META.no_host;
                const pointScopeLabel = group.points.length > 1
                    ? `Ponto geográfico em ${group.label}`
                    : "Ponto geográfico";
                const stationCount = Array.isArray(point.stations)
                    ? point.stations.length
                    : null;
                const stationCountLabel = stationCount === null
                    ? null
                    : `${stationCount} estação${stationCount === 1 ? "" : "ões"}`;
                const altitudeLabel = point.altitude !== null && point.altitude !== undefined
                    ? `Alt ${point.altitude} m`
                    : null;
                const gnssLabel = point.gnss_measurements
                    ? `${point.gnss_measurements} medições GNSS`
                    : null;
                const locationMode = isHistoricalPoint(point) ? "Histórico" : "Atual";
                const metaParts = [
                    altitudeLabel,
                    gnssLabel,
                    locationMode,
                ].filter(Boolean);
                const pointActionsHtml = buildPointActionAreaHtml(point, { cluster: true });

                return `
                    <div class="station-popup-cluster-point">
                        <div class="station-popup-cluster-point-kind">${escapeHtml(pointScopeLabel)}</div>
                        <div class="station-popup-cluster-point-header">
                            <span class="station-popup-cluster-point-name">${escapeHtml(`ID_SITE ${point.site_id}`)}</span>
                            <div class="station-popup-cluster-point-badges">
                                ${stationCountLabel ? `<span class="station-popup-cluster-point-count">${escapeHtml(stationCountLabel)}</span>` : ""}
                                <span class="station-popup-cluster-point-state state-${escapeHtml(stateKey)}">${escapeHtml(stateMeta.legendLabel)}</span>
                            </div>
                        </div>
                        <div class="station-popup-cluster-point-meta">${escapeHtml(metaParts.join(" · "))}</div>
                        ${pointActionsHtml}
                    </div>
                `;
            }).join("");

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
        const meta = getPointPopupMeta(point);
        const actionAreaHtml = buildPointActionAreaHtml(point);

        return `
            <div class="station-popup">
                <div class="station-popup-title">${escapeHtml(point.site_label)}</div>
                <div class="station-popup-meta">${escapeHtml(meta)}</div>
                ${actionAreaHtml}
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
     * Keep a popup inside the visible map bounds without letting Leaflet pan
     * the whole map on hover.
     *
     * `autoPan` is disabled to avoid the irritating map scroll/jump effect.
     * This helper compensates by nudging the popup element itself.
     */
    function fitPopupWithinMap(marker) {
        /* Hover popups should never drag the whole map with them.
         *
         * Leaflet's `autoPan` solves clipping by moving the viewport, but that
         * felt jittery and disorienting during exploration. The compromise
         * here is to keep the map still and reposition only the popup element
         * when it would bleed outside the visible container.
         */
        if (!marker) {
            return;
        }

        const popup = marker.getPopup();
        if (!popup || !popup.isOpen()) {
            return;
        }

        window.requestAnimationFrame(() => {
            const popupElement = popup.getElement();
            if (!popupElement) {
                return;
            }

            const baseTransform = popupElement.dataset.baseTransform || popupElement.style.transform || "";
            popupElement.dataset.baseTransform = baseTransform;
            popupElement.style.transform = baseTransform;

            const contentWrapper = popupElement.querySelector(".leaflet-popup-content-wrapper");
            if (contentWrapper) {
                const baseWrapperTransform = contentWrapper.dataset.baseTransform || contentWrapper.style.transform || "";
                contentWrapper.dataset.baseTransform = baseWrapperTransform;
                contentWrapper.style.transform = baseWrapperTransform;
            }

            const mapRect = map.getContainer().getBoundingClientRect();
            const popupRect = popupElement.getBoundingClientRect();
            const contentWrapperRect = contentWrapper
                ? contentWrapper.getBoundingClientRect()
                : popupRect;
            const padding = 14;
            let shiftX = 0;
            let shiftY = 0;

            if (contentWrapperRect.left < mapRect.left + padding) {
                shiftX = mapRect.left + padding - contentWrapperRect.left;
            } else if (contentWrapperRect.right > mapRect.right - padding) {
                shiftX = mapRect.right - padding - contentWrapperRect.right;
            }

            if (popupRect.top < mapRect.top + padding) {
                shiftY = mapRect.top + padding - popupRect.top;
            } else if (popupRect.bottom > mapRect.bottom - padding) {
                shiftY = mapRect.bottom - padding - popupRect.bottom;
            }

            if (shiftY) {
                const verticalTransform = `translateY(${Math.round(shiftY)}px)`;
                popupElement.style.transform = baseTransform
                    ? `${baseTransform} ${verticalTransform}`
                    : verticalTransform;
            }

            if (contentWrapper && shiftX) {
                const baseWrapperTransform = contentWrapper.dataset.baseTransform || contentWrapper.style.transform || "";
                const horizontalTransform = `translateX(${Math.round(shiftX)}px)`;
                contentWrapper.style.transform = baseWrapperTransform
                    ? `${baseWrapperTransform} ${horizontalTransform}`
                    : horizontalTransform;
            }
        });
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
        marker.setPopupContent(buildPopupHtml(point));
        fitPopupWithinMap(marker);
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
     * Bind hover and click behavior to the live popup DOM element.
     */
    function bindPopupElementInteractions(popupElement, cancelClose, scheduleClose) {
        if (!popupElement || popupElement.dataset.wfPopupBound === "1") {
            return;
        }

        popupElement.dataset.wfPopupBound = "1";
        popupElement.addEventListener("mouseenter", cancelClose);
        popupElement.addEventListener("mouseleave", scheduleClose);
    }

    /**
     * Keep open popups aligned with the current zoom-level nearby summary.
     */
    function refreshOpenPopupNearbyPoints() {
        renderedMarkers.forEach((marker) => {
            if (!marker.isPopupOpen()) {
                return;
            }

            refreshPopupNearbySummary(marker);

            if (marker.__wfPoint?.popupClusterSummary?.useCluster) {
                ensureClusterPointDetails(marker);
            } else {
                ensurePointDetails(marker.__wfPoint, marker);
            }
        });
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
                point.loadingDetails = false;
                point.detailsLoaded = true;
                point.detailsError = false;
                point.stations = Array.isArray(payload.stations) ? payload.stations : [];
                point.marker_state = payload.marker_state || point.marker_state || "no_host";
                point.has_online_station = Boolean(payload.has_online_station);
                point.has_online_host = point.has_online_station;
                point.has_known_host = Boolean(payload.has_known_host);
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

        marker.bindPopup(buildPopupHtml(point), {
            closeButton: false,
            autoClose: false,
            closeOnClick: false,
            autoPan: false,
            minWidth: 320,
            maxWidth: 400,
            className: "station-popup-container"
        });

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
                marker.openPopup();
            }, POPUP_HOVER_OPEN_DELAY_MS);
        }

        function scheduleClose() {
            cancelOpen();
            cancelClose();
            closeTimer = window.setTimeout(() => marker.closePopup(), 180);
        }

        marker.on("mouseover", function () {
            cancelClose();

            if (typeof marker.bringToFront === "function") {
                marker.bringToFront();
            }

            scheduleOpen();
        });

        marker.on("mouseout", scheduleClose);

        marker.on("popupopen", function (event) {
            closeOtherPopups(marker);
            refreshPopupNearbySummary(marker);
            if (point.popupClusterSummary?.useCluster) {
                ensureClusterPointDetails(marker);
            } else {
                ensurePointDetails(point, marker);
            }
            const popupElement = marker.getPopup()?.getElement() || event.popup.getElement();
            if (!popupElement) {
                return;
            }

            // Leaflet recreates popup DOM on each open, so the hover listeners
            // and base transform need to be rebound to the live element every
            // time the popup mounts.
            popupElement.dataset.baseTransform = popupElement.style.transform || "";
            fitPopupWithinMap(marker);
            bindPopupElementInteractions(popupElement, cancelClose, scheduleClose);
        });

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
        // The site combobox is state-aware: changing the state filter rebuilds
        // the candidate list so users do not see impossible station choices.
        //
        // We also disambiguate duplicate human labels here, because this is
        // the one place where we have full context to keep the selector clean
        // without leaking that complexity into later lookup/render paths.
        const filteredByState = selectedStateCode
            ? points.filter((point) => point.state_code === selectedStateCode)
            : points;
        const sortedPoints = sortSites(filteredByState);
        const baseLabelCounts = new Map();

        sortedPoints.forEach((point) => {
            const baseLabel = getPointOptionBaseLabel(point);
            baseLabelCounts.set(baseLabel, (baseLabelCounts.get(baseLabel) || 0) + 1);
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
                label: optionLabel
            });
            siteOptionIndex.set(normalizeSearchText(optionLabel), String(point.site_id));
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
        // can get back to the unfiltered state even while typing in the box.
        //
        // The menu is intentionally capped because the combobox is meant to be
        // a practical operational picker, not an infinite-scroll directory.
        // Search correctness still comes from the normalized option index, so
        // limiting the rendered subset does not break exact resolution.
        const searchTerm = normalizeSearchText(siteFilter.value);
        const visibleOptions = siteOptionRecords.filter((option) => {
            if (!option.site_id) {
                return true;
            }

            if (!searchTerm) {
                return true;
            }

            return normalizeSearchText(option.label).includes(searchTerm);
        }).slice(0, 120);
        // Cap visible options to keep DOM work bounded. Exact matches still
        // resolve correctly because `siteOptionIndex` is independent of this
        // rendered subset.

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
        /* Selection is synchronized in three places:
         * - the internal exact `selectedSiteId`
         * - the visible combobox text
         * - the filtered marker render
         *
         * Keeping that synchronization explicit here makes the combobox easier
         * to reason about than spreading partial updates across multiple input
         * and click handlers.
         */
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
        // Filtering order matters only for readability here, not correctness:
        // state -> exact selected site -> free-text site search -> status -> locality.
        //
        // Those layers are intentionally orthogonal:
        // - geography (`Estado`)
        // - exact target (`Estação`)
        // - semantic scope (`Status`, `Localidades`)
        // That keeps the toolbar mentally consistent and prevents one control
        // from silently changing the meaning of another.
        const selectedStateCode = stateFilter.value;
        const activeSiteId = getSelectedSiteId();
        const siteSearchTerm = activeSiteId
            ? ""
            : normalizeSearchText(siteFilter.value) === normalizeSearchText(ALL_SITES_LABEL)
            ? ""
            : normalizeSearchText(siteFilter.value);
        const statusScope = statusFilter ? statusFilter.value : "all";
        const localityScope = localityFilter ? localityFilter.value : "include_history";

        return allStationPoints.filter((point) => {
            if (selectedStateCode && point.state_code !== selectedStateCode) {
                return false;
            }

            if (activeSiteId) {
                if (String(point.site_id) !== activeSiteId) {
                    return false;
                }
            } else if (siteSearchTerm && !getPointSearchText(point).includes(siteSearchTerm)) {
                return false;
            }

            if (statusScope === "online_only" && !isOnlinePoint(point)) {
                return false;
            }

            if (statusScope === "offline_only" && !isOfflinePoint(point)) {
                return false;
            }

            if (localityScope === "current_only" && isHistoricalPoint(point)) {
                return false;
            }

            if (localityScope === "historical_only" && !isHistoricalPoint(point)) {
                return false;
            }

            return true;
        });
    }

    /**
     * Re-render the markers, legend and map bounds from the current control
     * state.
     *
     * This is the single visual refresh entrypoint used by every control so
     * the map, point count, legend and zoom behavior stay consistent.
     */
    function renderFilteredPoints() {
        // Re-render also refreshes the legend and map bounds so every control
        // change produces one coherent visual update.
        //
        // This is the central reconciliation step of the home map: every
        // filter or combobox change funnels through here so the markers,
        // legend, counter and zoom behavior always reflect the same state.
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

        if (filteredPoints.length > 0 && bounds.length > 0) {
            /* Zoom behavior is intentionally contextual:
             * - when one station/locality is explicitly selected, zoom in
             *   enough to make that target feel chosen
             * - otherwise fit the filtered extent while preserving the more
             *   exploratory character of the home map
             */
            map.fitBounds(bounds, {
                padding: [30, 30],
                // A single explicit site selection signals stronger navigation
                // intent than browsing all points, so allow a tighter zoom.
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
            });
    }

    // ---------------------------------------------------------------------
    // Event wiring
    // ---------------------------------------------------------------------
    // Event wiring is intentionally explicit because the toolbar mixes native
    // selects with a custom searchable combobox. Each control keeps the
    // interaction model users would expect from that specific UI pattern.
    //
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

    if (startDateFilter) {
        startDateFilter.addEventListener("change", () => {
            loadStationPoints();
        });
    }

    if (endDateFilter) {
        endDateFilter.addEventListener("change", () => {
            loadStationPoints();
        });
    }

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
    });

    // ---------------------------------------------------------------------
    // Initial load
    // ---------------------------------------------------------------------
    // Startup order matters:
    //   1. restore the persisted basemap
    //   2. wire the toolbar interactions
    //   3. fetch the summary point payload
    //
    // That sequence ensures the first visible render already respects the
    // operator's saved theme and the controls are live before data arrives.
    applyMapTheme(loadSavedMapTheme());

    // The first payload is intentionally summary-only. Full station actions are
    // deferred to the lazy popup requests so the home page stays responsive.
    loadStationPoints();
})();
