# Xpedit - Runbook de Incidentes

## Servicios y URLs

| Servicio | URL | Dashboard |
|----------|-----|-----------|
| Backend API | https://web-production-94783.up.railway.app | Railway Dashboard |
| Website | https://xpedit.es | Vercel Dashboard |
| Base de datos | Supabase (oantjoyexhscoemzfeae) | supabase.com/dashboard |
| Monitoring | Sentry (taes-pack-sl) | taes-pack-sl.sentry.io |
| Email | Resend | resend.com/emails |

## Health Check

```
GET https://web-production-94783.up.railway.app/health
```

Respuesta esperada (200):
```json
{
  "status": "healthy",
  "checks": {
    "database": {"status": "ok"},
    "sentry": {"status": "ok"},
    "scheduler": {"status": "ok"},
    "uptime_seconds": 12345,
    "version": "1.1.3"
  }
}
```

Si devuelve 503: el backend esta degradado (alguna dependencia falla).

---

## Incidencia 1: Backend no responde (502/503)

**Sintomas**: La app no carga datos, errores de red, /health devuelve error o no responde.

**Pasos**:
1. Verificar Railway Dashboard - ver si el servicio esta running
2. Revisar logs en Railway: buscar errores de inicio (import errors, env vars faltantes)
3. Si hay crash loop: ver ultimo commit, hacer rollback en Railway si necesario
4. Verificar variables de entorno (SUPABASE_URL, SUPABASE_KEY, etc.)
5. Si es OOM (Out of Memory): revisar Railway metrics, considerar upgrade de plan

**Mitigacion rapida**: Railway permite rollback a cualquier deploy anterior desde el dashboard.

---

## Incidencia 2: Base de datos no accesible

**Sintomas**: /health devuelve database: error, la app no guarda datos.

**Pasos**:
1. Verificar Supabase Dashboard - estado del proyecto
2. Comprobar si el proyecto esta pausado (free tier se pausa por inactividad)
3. Si pausado: click "Restore" en Supabase Dashboard
4. Verificar que SUPABASE_URL y SUPABASE_KEY son correctos en Railway
5. Verificar que no se ha superado el limite de conexiones (free: 60 connections)

**Prevencion**: Los backups automaticos (3:00 UTC diario) guardan datos en Storage.

---

## Incidencia 3: Errores masivos en la app (crashes)

**Sintomas**: Usuarios reportan crashes, Sentry muestra pico de errores.

**Pasos**:
1. Revisar Sentry Issues - identificar el error mas frecuente
2. Ver en que version/dispositivo ocurre (Sentry muestra contexto)
3. Si es un error de la API: verificar backend logs
4. Si es un error de la app: preparar hotfix y OTA update
5. OTA update: `eas update --branch production && eas update --branch preview`

**Nota**: OTA solo funciona para cambios JS. Cambios nativos requieren nuevo build.

---

## Incidencia 4: Emails no se envian

**Sintomas**: Usuarios no reciben emails de bienvenida/verificacion.

**Pasos**:
1. Verificar Resend Dashboard (resend.com/emails) - ver si hay bounces
2. Verificar RESEND_API_KEY en Railway
3. Comprobar el dominio xpedit.es en Resend - verificar DNS records
4. Verificar ImprovMX para recepcion (improvmx.com)
5. Probar envio manual: POST /email/welcome con token admin

---

## Incidencia 5: Posts sociales no se publican

**Sintomas**: Posts programados no se publican a la hora.

**Pasos**:
1. /health - verificar scheduler status = "ok"
2. Revisar logs Railway - buscar "Social scheduler error"
3. Verificar tokens: TWITTER_CONSUMER_KEY, LINKEDIN_ACCESS_TOKEN
4. LinkedIn token expira cada 2 meses (~16 Abril 2026) - renovar manualmente
5. Si el scheduler no arranca: reiniciar el servicio en Railway

---

## Incidencia 6: Backup fallido

**Sintomas**: Sentry alerta "daily-backup" con status error.

**Pasos**:
1. Revisar logs Railway - buscar "Backup failed"
2. Verificar espacio en Supabase Storage (free: 1GB)
3. Los backups estan en: Storage > social-media > backups/YYYY-MM-DD/
4. Si Storage lleno: eliminar backups antiguos (> 30 dias)
5. Ejecutar backup manual: reiniciar el servicio (ejecutara al siguiente cron)

---

## Incidencia 7: Google Play / Apple rechazo

**Sintomas**: Notificacion de rechazo en la consola.

**Pasos**:
1. Leer el motivo de rechazo completo
2. Motivos frecuentes:
   - Prominent Disclosure: revisar textos del Alert de permisos
   - Screenshots: deben coincidir con la version actual
   - Privacy Policy: debe ser accesible en https://xpedit.es/privacy
3. Corregir y subir nuevo build (incrementar versionCode/buildNumber)
4. Responder al equipo de revision si piden mas informacion

---

## Contactos

| Quien | Email | Rol |
|-------|-------|-----|
| Miguel Angel | migue995@gmail.com | Developer / Admin |
| Soporte | info@xpedit.es | Email publico |
| TAES PACK S.L. | direccion@taespack.com | Empresa |

---

## Jobs Programados

| Job | Frecuencia | Hora (UTC) | Sentry Monitor |
|-----|-----------|------------|----------------|
| Social post checker | Cada 60s | Continuo | - |
| Backup tablas criticas | Diario | 03:00 | daily-backup |
| Retention cleanup | Semanal (Dom) | 04:00 | weekly-retention-cleanup |

---

## Rollback Rapido

**Backend (Railway)**:
1. Dashboard > Deployments > click en deploy anterior > Rollback

**Website (Vercel)**:
1. Dashboard > Deployments > click en deploy anterior > Promote to Production

**App (OTA)**:
```bash
# Revertir a version anterior de OTA
eas update:rollback --branch production
eas update:rollback --branch preview
```

**App (build nativo)**: No se puede rollback. Hay que subir nueva version a las stores.
