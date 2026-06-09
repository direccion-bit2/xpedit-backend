"""routingPreference por origen (#45, ahorro Routes Pro→Essentials, 26 may 2026).

Contrato: las calls de PREVIEW de geometría (resume/optimize/cold-start/...) usan
TRAFFIC_UNAWARE = SKU "Compute Routes Essentials" ($5/1k, 10k gratis). La navegación
ACTIVA y cualquier source desconocido usan TRAFFIC_AWARE = SKU "Compute Routes Pro"
($10/1k, 5k gratis), donde el ETA con tráfico y esquivar atascos/cortes sí importan.

Si alguien añade un source de conducción a _ROUTES_V2_UNAWARE_PREFIXES por error,
estos tests fallan — eso degradaría la navegación real del driver.
"""
import main


class TestRoutesV2RoutingPref:
    def test_preview_sources_use_unaware_essentials(self):
        for src in ("optimize", "load-route", "cold-start", "resume", "invert", "auto-effect"):
            assert main._routes_v2_routing_pref(src) == "TRAFFIC_UNAWARE", src

    def test_suffixed_preview_sources_still_unaware(self):
        # El cliente RN (useRoutes.ts) añade sufijos -401retry/-429retry/-returnleg/-jwt-retry.
        for src in (
            "resume-401retry",
            "optimize-returnleg",
            "cold-start-cloud",
            "optimize-jwt-retry",
            "load-route-429retry",
        ):
            assert main._routes_v2_routing_pref(src) == "TRAFFIC_UNAWARE", src

    def test_active_navigation_stays_aware_pro(self):
        # start-nav = navegación activa → DEBE seguir en Pro (tráfico en tiempo real).
        assert main._routes_v2_routing_pref("start-nav") == "TRAFFIC_AWARE"
        assert main._routes_v2_routing_pref("start-nav-401retry") == "TRAFFIC_AWARE"

    def test_recalc_downgraded_to_essentials_on_purpose(self):
        # (#64, 10 jun) recalc → Essentials es DECISIÓN, no error: el reroute tras
        # desvío solo necesita geometría nueva (el ETA real lo da el GPS en vivo)
        # y era el componente más frecuente del coste de nav (855 calls Pro/5d).
        # Misma geometría, mitad de precio. Si esto falla, alguien revirtió el
        # ahorro — confirmar con Miguel antes de tocar.
        assert main._routes_v2_routing_pref("recalc") == "TRAFFIC_UNAWARE"
        assert main._routes_v2_routing_pref("recalc-jwt-retry") == "TRAFFIC_UNAWARE"

    def test_unknown_and_empty_default_to_aware(self):
        # Conservador: ante la duda, Pro — nunca degradar la conducción por un source nuevo.
        for src in ("unknown", "", "reroute", "off-route", None):
            assert main._routes_v2_routing_pref(src) == "TRAFFIC_AWARE", repr(src)

    def test_case_and_whitespace_insensitive(self):
        assert main._routes_v2_routing_pref("  RESUME  ") == "TRAFFIC_UNAWARE"
        assert main._routes_v2_routing_pref("Optimize") == "TRAFFIC_UNAWARE"
