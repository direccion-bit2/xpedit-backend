#!/bin/bash
# RutaMax - Script de inicio del backend

cd "$(dirname "$0")"
source venv/bin/activate

echo "🚀 Iniciando RutaMax API..."
echo "📍 URL: http://localhost:8004"
echo "📚 Docs: http://localhost:8004/docs"
echo ""
echo "Presiona Ctrl+C para detener"
echo ""

uvicorn main:app --host 0.0.0.0 --port 8004 --reload
