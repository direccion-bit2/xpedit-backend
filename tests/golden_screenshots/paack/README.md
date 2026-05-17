# Paack — Golden Dataset

Etiquetas físicas del carrier **Paack** (España, zona León). Fotos aportadas por **Beatriz** (driver de Miguel) el 17 may 2026.

## Estructura

```
paack/
├── README.md              (este archivo)
├── ground_truth.json      (11 etiquetas con extracción esperada)
└── raw/                   (fotos físicas — UUID filenames)
```

## Origen del dataset

- **Fuente**: 17 fotos enviadas por Beatriz a Miguel el 17 may 2026 madrugada.
- **Útiles**: 11 etiquetas Paack legibles.
- **Descartadas**: 1 imagen demasiado doblada/ilegible + 5 imágenes de otros carriers (Sending, UPS, Amazon-UPS no-Paack).
- **Zona geográfica**: 100% provincia de León (CPs 24001-24391). Esto es una limitación geográfica — al igual que Zeleris quedó anclado a Conil/Cádiz, Paack queda anclado a León. Futuro: pedir etiquetas de otras provincias.

## Variantes Paack detectadas

| Variante | Cuándo aparece | Layout |
|---|---|---|
| **CFA** | Productos Anbo/Shein (origen China FO SHAN) | Badge `CFA` esquina + barcode lateral + wave (5 dígitos) + Prologis Anbo como sender |
| **NT4** | Productos Nike (origen Montcada i Reixac) | Badge `NT4` esquina + barcode arriba + wave + tracking 9 dígitos · ciudad puede aparecer truncada (`Len` por `León`) |
| **ECI** | El Corte Inglés | Layout form-style "Datos de Envío" · campos Operación venta + Pedido + Bulto + Oleada · texto "PAACK" en medio inferior · NO barcode lateral |
| **co-branded Tiendanimal** | Distribución Tiendanimal vía Paack | Layout Tiendanimal completo (DHL Supply Chain como origen) con badge `paack` arriba derecha + "Next-day delivery / Timeslot available" |
| **Amazon-Paack** | Distribución Amazon vía Paack | Layout Amazon estándar con badge Paack arriba derecha + datamatrix grid + códigos MAD4/A266 (sortation) |

## Gotchas críticos del extractor

1. **Origen FO SHAN CN / Nike Montcada / Amazon SFH / Tiendanimal Ontigola = SENDER, NO destino**. El destinatario real está en bloque inferior con su nombre + dirección + CP + ciudad.
2. **Wave number (24391, 24350, etc.)** es el número grande arriba derecha. Coincide visualmente con CP pero es CÓDIGO INTERNO DE PAACK, NO postal code. El CP real está en el bloque del destinatario.
3. **`CFA` / `NT4`** son flags de tipo de servicio Paack, NO ciudad ni código postal.
4. **Manuscritas sobreescriben impresas**: ejemplo en `paack_04_adriana_escalona`, dirección impresa "Avenida de Portugal 7" tachada y manuscrita "Rep Argentina 31 Pdo" — la manuscrita gana siempre.
5. **`Len` debe normalizarse a `León`** (typo común en label NT4 por truncamiento del generador).
6. **`Pdo` / `P.Bajo` / `Pl Bajo` / `Mapfre`** → van en `floor_etc`, no en street. Son piso/portal/referencia local.
7. **Urbanizaciones sin número convencional** (ej. Chalet 58): `number` queda vacío, identificador completo en `floor_etc`.
8. **Etiquetas rotadas 90/180/270°** — común en CFA (bolsas grandes). NUNCA descartar por rotación.
9. **Pedanías de León**: Carbajal de la Legua = Sariegos area (CP 24196), Espinosa de la Ribera = pedanía (24274), Aldea de la Valdoncina (24391), San Miguel del Camino (24391), Villarejo de Orbigo (24350).
10. **`n12` = número 12** (`n` es abreviatura). Extraer solo el dígito.

## Tabla rápida de las 11 etiquetas

| # | UUID | Destinatario | CP | Ciudad | Variante | Rot | Edge case |
|---|---|---|---|---|---|---|---|
| 01 | 670a75bf… | Camino Fernandez Gutierrez | 24391 | San Miguel del Camino | CFA | 270° | Rotación |
| 02 | 6b47aa78… | Marina Fidalgo | 24391 | Aldea de la Valdoncina | NT4 | 0° | — |
| 03 | 331e56e8… | Nisrine Slihimi | 24350 | Villarejo de Orbigo | CFA | 0° | floor_etc separado |
| 04 | 3799edb6… | Adriana Escalona | 24009 | León | CFA | 0° | **Manuscrita override** |
| 05 | 646eb296… | Alfonso González Gutiérrez | 24002 | León | ECI | 0° | Layout form-style |
| 06 | 39339593… | Álvarez Villa M. Cristina | 24001 | León | NT4 | 90° | **`Len`→`León`** |
| 07 | 18014b6f… | Diez Estévez Marian | 24001 | León | NT4 | 0° | Apellido invertido |
| 08 | 15a660ac… | Sandra García Cañón | 24001 | León | co-branded | 0° | Tiendanimal layout |
| 09 | bad75f40… | Maria Muniz | 24121 | Sariegos | NT4 | 0° | `n12` parse |
| 10 | 929723ee… | Veronica Aller Acebes | 24274 | Espinosa de la Ribera | CFA | 0° | Pedanía rural |
| 11 | 2967c1b2… | Elena González Morán | 24196 | Carbajal de la Legua | Amazon-Paack | 0° | Chalet sin nº |

## Cómo correr la evaluación

```bash
cd backend
pytest tests/golden_ocr_runner.py -v -k paack
```

(Misma mecánica que Zeleris: el runner compara la extracción Gemini real contra `ground_truth.json` y reporta precisión por carrier.)

## Roadmap

- [ ] Fase 1 (HOY): few-shot inyectado en `_msi_build_prompt('paack', ...)` con 3 ejemplos (CFA-Anbo, NT4-Nike, ECI-ECI). Esperado: 60-80% precisión baseline.
- [ ] Fase 2: pytest continuo en CI cuando se toque el prompt (igual que Zeleris #260).
- [ ] Fase 3: alerta Sentry si precisión Paack baja del 80% en prod (igual que Zeleris #261).
- [ ] Fase 4: pedir a Beatriz etiquetas Paack de otras provincias (Madrid, Sevilla, Barcelona) para evitar el sesgo geográfico León.
