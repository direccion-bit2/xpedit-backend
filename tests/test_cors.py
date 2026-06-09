"""CORS preview-origin regex: permitir SOLO los previews de Vercel de este
proyecto/cuenta, nunca *.vercel.app de terceros ni dominios maliciosos."""
import re

from main import VERCEL_PREVIEW_ORIGIN_REGEX

_pat = re.compile(VERCEL_PREVIEW_ORIGIN_REGEX)


class TestVercelPreviewOriginRegex:
    def test_permite_previews_del_proyecto(self):
        allowed = [
            "https://website-id8eno9td-miguels-projects-547e23cc.vercel.app",
            "https://website-git-feat-empresa-panel-9dd9f5-miguels-projects-547e23cc.vercel.app",
        ]
        for origin in allowed:
            assert _pat.match(origin), f"debería permitir {origin}"

    def test_rechaza_vercel_de_terceros_y_dominios_maliciosos(self):
        denied = [
            "https://website-abc-otra-cuenta.vercel.app",            # otra cuenta Vercel
            "https://evil-miguels-projects-547e23cc.vercel.app",     # no empieza por website-
            "https://website-abc-miguels-projects-547e23cc.vercel.app.evil.com",  # sufijo falso
            "http://website-abc-miguels-projects-547e23cc.vercel.app",  # http, no https
            "https://xpedit.es.evil.com",
            "https://anything.vercel.app",
        ]
        for origin in denied:
            assert not _pat.match(origin), f"NO debería permitir {origin}"
