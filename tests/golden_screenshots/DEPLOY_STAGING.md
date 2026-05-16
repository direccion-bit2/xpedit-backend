# MSI — plan de deploy a staging

Backend `feature/multi-screenshot-importer` (commits `e1d80b0` + `e1b8a97`)
está listo y pusheado, **sin desplegar**. Su deploy a staging no se ha
hecho automático porque la rama `staging` lleva tiempo desincronizada con
`main` (versiones distintas de `emails.py`, `optimizer.py`, etc.) y un
merge ciego provocaría conflictos múltiples + posible rotura del
endpoint `/ocr/label` en staging para drivers de prueba.

## Opción recomendada (15 min, coordinada con Miguel)

**Reset hard de `staging` al `main` actual + merge feature limpio**, ya
que `staging` actualmente solo aporta variantes antiguas de archivos que
están sustituidas por versiones más recientes en `main`.

Pasos:

```bash
# 1. Sanity: compara qué tenemos en cada rama
cd backend
git fetch origin
git log --oneline origin/staging..origin/main | head      # commits que main tiene y staging no
git log --oneline origin/main..origin/staging | head      # commits únicos de staging

# 2. Si "commits únicos de staging" no aporta nada útil (el caso actual),
#    reset hard staging al main:
git checkout staging
git reset --hard origin/main
git merge feature/multi-screenshot-importer --no-ff -m "msi: MVP backend (Days 1+2) to staging"
git push --force-with-lease origin staging

# 3. Railway redeploy automático (~2 min). Vigilar:
curl -s https://web-staging-5f41.up.railway.app/health
# El endpoint nuevo:
curl -X POST https://web-staging-5f41.up.railway.app/ocr/screenshots-batch \
  -H "Authorization: Bearer <staging JWT>" \
  -H "Content-Type: application/json" \
  -d '{"images":[{"image_base64":"AAAA","media_type":"image/jpeg"}]}'
# → debería responder 403 pro_plus_required si el user no tiene Pro+/trial,
#   o 200 si sí.
```

`--force-with-lease` solo fuerza si NADIE más ha empujado a staging entre
medias (más seguro que `--force`).

## App OTA staging

Una vez el backend staging está vivo:

```bash
cd app
git checkout feature/multi-screenshot-importer
./scripts/ota-staging.sh "feat(msi): MVP — backend + app review screen"
```

El script `ota-staging.sh` corre `eas update --branch staging` que
empuja a `com.taespack.rutamax.dev` (la app naranja DEV de Miguel),
**no afecta al app de los drivers**.

## Smoke test (Miguel)

1. Abrir DEV app, asegurarse cuenta tiene Pro+ o trial activo.
2. Pulsar "+" → ver nueva tarjeta "Importar pantallazos · Pro+".
3. Hacer 2-3 screenshots de tu app de paquetería (CTT real).
4. Subirlos, marcar carrier="ctt", procesar.
5. Verificar:
   - Lista de paradas con chips verdes/amarillas/rojas
   - Tap-hold sobre una parada muestra la foto original
   - Swipe-left elimina con undo 5s
   - Tap card expande edit inline
   - CTA habilitado solo si 0 rojas pendientes
   - Pulsar "Crear ruta" → paradas aparecen en el mapa

## Si el smoke test pasa

Crear PR `feature/multi-screenshot-importer` → `main` en GitHub. **NO
auto-merge**. Esperamos OK explícito de Miguel después de uso real.

## Si el smoke test falla

- **Backend**: rollback Railway al deploy anterior desde el dashboard
  Railway. La rama `staging` queda con el cambio pero el servicio no
  sirve el código nuevo.
- **App**: rollback OTA staging — `eas update:republish --group <ID
  anterior> --branch staging`. La cuenta DEV vuelve al bundle previo.
- Recoger Sentry breadcrumbs `msi.*` para diagnosticar y volver a
  iterar en el feature branch.
