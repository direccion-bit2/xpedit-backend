# Zeleris ZLR Día Siguiente — Golden Dataset

Etiquetas físicas del carrier **Zeleris** (servicio "ZLR DIA SIGUIENTE", zona Cádiz/Conil).
Patrón visual: banner verde-lima con barcode arriba, bloque blanco con destinatario+dirección+CP+ciudad debajo, "ZONA DE REPARTO" en cuadro a la derecha, logo Zeleris (con puntos verdes) abajo.

## Estructura

Cada etiqueta tiene 2 archivos:
- `zeleris_NN.jpg` — la foto física (driver añade manualmente desde su iPhone)
- `zeleris_NN.json` — ground truth (campos extraídos a mano)

## Fotos capturadas (8 etiquetas — Miguel 16 may 2026)

Pendiente que Miguel suba las JPG físicas a este directorio. Las ground truths ya están escritas en los .json.

| # | Destinatario | CP | Ciudad | Notas extracción Gemini 16 may |
|---|---|---|---|---|
| 01 | Elena María Salas Perdigones | 11580 | San José del Valle | ❌ Falló (etiqueta rotada 180°) |
| 02 | María José Del Carmen Parodi Leal | 11140 | Conil de la Frontera | ❌ Falló |
| 03 | CADIZFORNIA (comercio) | 11140 | Conil de la Frontera | ❌ Falló |
| 04 | SRN Vertice Group SL (Jesús Silva) | 11140 | Conil de la Frontera | ✅ Amarilla (C. Extramuros 2) |
| 05 | (Av. Dolores Ibárruri Conil) | 11140 | Conil de la Frontera | ✅ Verde |
| 06 | Luis Miguel Martín Sánchez | 11149 | Conil de la Frontera | ❌ Falló (etiqueta cortada) |
| 07 | Olga García Esteve | 11140 | Conil de la Frontera | (nuevo — post mejora prompt) |
| 08 | (Jesús Silva — repetida?) | 11140 | Conil de la Frontera | (nuevo — post mejora prompt) |

## Cómo subir las JPG

Cuando Miguel exporte las 8 fotos desde su iPhone (AirDrop/iCloud/email a `direccion@taespack.com`):
1. Renombrar a `zeleris_01.jpg` ... `zeleris_08.jpg` (orden de la tabla arriba)
2. Mover a este directorio
3. Ejecutar `pytest tests/golden_ocr_runner.py -v -k zeleris` → evalúa contra el ground truth

## Para qué sirve

- **Few-shot examples** (tarea #256): los primeros 3-4 .jpg+.json se inyectan en el prompt como ejemplos resueltos, así Gemini aprende el layout Zeleris específicamente
- **Eval continuo** (tarea #260): cada vez que tocamos el prompt, run del runner → métrica de precisión por carrier
- **Sentry post-launch** (tarea #261): si la precisión Zeleris baja del 80% en prod, alerta
