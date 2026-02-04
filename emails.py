"""
Xpedit - Servicio de emails con Resend
"""

import os
import resend
from typing import Optional

# Configurar API key
resend.api_key = os.getenv("RESEND_API_KEY")

# Email de envío (dominio verificado)
FROM_EMAIL = "Xpedit <notificaciones@xpedit.es>"


def get_base_template(content: str, title: str = "Xpedit") -> str:
    """Template base HTML para todos los emails"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
    </head>
    <body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f4f4f5;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f4f4f5; padding: 40px 20px;">
            <tr>
                <td align="center">
                    <table width="100%" style="max-width: 600px; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                        <!-- Header -->
                        <tr>
                            <td style="background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%); padding: 30px; text-align: center;">
                                <h1 style="margin: 0; color: #ffffff; font-size: 28px; font-weight: 700;">Xpedit</h1>
                                <p style="margin: 8px 0 0 0; color: rgba(255,255,255,0.9); font-size: 14px;">Optimiza tus rutas de reparto</p>
                            </td>
                        </tr>
                        <!-- Content -->
                        <tr>
                            <td style="padding: 40px 30px;">
                                {content}
                            </td>
                        </tr>
                        <!-- Footer -->
                        <tr>
                            <td style="background-color: #f9fafb; padding: 25px 30px; text-align: center; border-top: 1px solid #e5e7eb;">
                                <p style="margin: 0 0 10px 0; color: #6b7280; font-size: 13px;">
                                    Este email fue enviado por Xpedit
                                </p>
                                <p style="margin: 0; color: #9ca3af; font-size: 12px;">
                                    <a href="https://xpedit.es" style="color: #22c55e; text-decoration: none;">xpedit.es</a> |
                                    <a href="https://xpedit.es/legal/privacidad" style="color: #22c55e; text-decoration: none;">Privacidad</a>
                                </p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


def send_welcome_email(to_email: str, user_name: str) -> dict:
    """Email de bienvenida para nuevos usuarios"""
    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px;">¡Bienvenido a Xpedit, {user_name}!</h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Gracias por unirte a Xpedit. Estamos encantados de tenerte con nosotros.
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Con Xpedit podrás:
        </p>

        <ul style="margin: 0 0 25px 0; padding-left: 20px; color: #4b5563; font-size: 15px; line-height: 1.8;">
            <li><strong>Optimizar rutas</strong> - Ahorra tiempo y combustible</li>
            <li><strong>Seguimiento en tiempo real</strong> - Sabe dónde están tus repartidores</li>
            <li><strong>Pruebas de entrega</strong> - Fotos y firmas digitales</li>
            <li><strong>Notificaciones automáticas</strong> - Mantén informados a tus clientes</li>
        </ul>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es/dashboard" style="display: inline-block; background-color: #22c55e; color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 8px; font-weight: 600; font-size: 16px;">
                Ir al Dashboard
            </a>
        </div>

        <p style="margin: 25px 0 0 0; color: #6b7280; font-size: 14px;">
            ¿Necesitas ayuda? Escríbenos por WhatsApp al +34 632 073 689
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": f"¡Bienvenido a Xpedit, {user_name}!",
            "html": get_base_template(content, "Bienvenido a Xpedit")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_delivery_started_email(
    to_email: str,
    client_name: str,
    driver_name: str,
    estimated_time: Optional[str] = None,
    tracking_url: Optional[str] = None
) -> dict:
    """Email cuando el pedido está en camino"""

    time_text = f"<p style='margin: 0 0 20px 0; color: #4b5563; font-size: 16px;'>Tiempo estimado de llegada: <strong>{estimated_time}</strong></p>" if estimated_time else ""

    tracking_button = ""
    if tracking_url:
        tracking_button = f"""
        <div style="text-align: center; margin: 30px 0;">
            <a href="{tracking_url}" style="display: inline-block; background-color: #22c55e; color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 8px; font-weight: 600; font-size: 16px;">
                Seguir mi pedido
            </a>
        </div>
        """

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #dcfce7; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">🚚</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            ¡Tu pedido está en camino!
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{client_name}</strong>,
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Tu pedido ha salido para entrega. <strong>{driver_name}</strong> está en camino para llevártelo.
        </p>

        {time_text}

        {tracking_button}

        <div style="background-color: #f0fdf4; border-radius: 8px; padding: 15px; margin-top: 20px;">
            <p style="margin: 0; color: #166534; font-size: 14px;">
                <strong>Consejo:</strong> Asegúrate de estar disponible para recibir tu pedido.
            </p>
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": "🚚 Tu pedido está en camino",
            "html": get_base_template(content, "Pedido en camino")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_delivery_completed_email(
    to_email: str,
    client_name: str,
    delivery_time: str,
    photo_url: Optional[str] = None,
    recipient_name: Optional[str] = None
) -> dict:
    """Email de confirmación de entrega"""

    photo_section = ""
    if photo_url:
        photo_section = f"""
        <div style="margin: 25px 0; text-align: center;">
            <p style="margin: 0 0 10px 0; color: #6b7280; font-size: 14px;">Foto de entrega:</p>
            <img src="{photo_url}" alt="Prueba de entrega" style="max-width: 100%; border-radius: 8px; border: 1px solid #e5e7eb;">
        </div>
        """

    recipient_text = f"<p style='margin: 0 0 15px 0; color: #4b5563; font-size: 15px;'>Recibido por: <strong>{recipient_name}</strong></p>" if recipient_name else ""

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #dcfce7; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">✅</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            ¡Pedido entregado!
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{client_name}</strong>,
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Tu pedido ha sido entregado correctamente.
        </p>

        <div style="background-color: #f9fafb; border-radius: 8px; padding: 20px; margin: 20px 0;">
            <p style="margin: 0 0 15px 0; color: #4b5563; font-size: 15px;">
                Fecha y hora: <strong>{delivery_time}</strong>
            </p>
            {recipient_text}
        </div>

        {photo_section}

        <p style="margin: 25px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            ¡Gracias por confiar en nosotros!
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": "✅ Tu pedido ha sido entregado",
            "html": get_base_template(content, "Pedido entregado")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_delivery_failed_email(
    to_email: str,
    client_name: str,
    reason: Optional[str] = None,
    next_attempt: Optional[str] = None
) -> dict:
    """Email cuando la entrega no se pudo completar"""

    reason_text = f"<p style='margin: 0 0 20px 0; color: #4b5563; font-size: 16px;'><strong>Motivo:</strong> {reason}</p>" if reason else ""
    next_text = f"<p style='margin: 0 0 20px 0; color: #4b5563; font-size: 16px;'>Próximo intento: <strong>{next_attempt}</strong></p>" if next_attempt else ""

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #fef2f2; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">📦</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            No pudimos entregar tu pedido
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{client_name}</strong>,
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hemos intentado entregar tu pedido pero no ha sido posible completar la entrega.
        </p>

        {reason_text}
        {next_text}

        <div style="background-color: #fef3c7; border-radius: 8px; padding: 15px; margin-top: 20px;">
            <p style="margin: 0; color: #92400e; font-size: 14px;">
                <strong>¿Necesitas ayuda?</strong> Contacta con nosotros por WhatsApp al +34 632 073 689
            </p>
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": "📦 No pudimos entregar tu pedido",
            "html": get_base_template(content, "Entrega no completada")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_daily_summary_email(
    to_email: str,
    dispatcher_name: str,
    date: str,
    total_routes: int,
    total_stops: int,
    completed_stops: int,
    failed_stops: int
) -> dict:
    """Email de resumen diario para dispatchers"""

    success_rate = round((completed_stops / total_stops * 100) if total_stops > 0 else 0, 1)

    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px;">
            Resumen del día - {date}
        </h2>

        <p style="margin: 0 0 25px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{dispatcher_name}</strong>, aquí tienes el resumen de entregas de hoy:
        </p>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 25px;">
            <tr>
                <td width="50%" style="padding: 15px; background-color: #f0fdf4; border-radius: 8px 0 0 8px; text-align: center;">
                    <p style="margin: 0; color: #166534; font-size: 32px; font-weight: 700;">{total_routes}</p>
                    <p style="margin: 5px 0 0 0; color: #166534; font-size: 14px;">Rutas</p>
                </td>
                <td width="50%" style="padding: 15px; background-color: #eff6ff; border-radius: 0 8px 8px 0; text-align: center;">
                    <p style="margin: 0; color: #1e40af; font-size: 32px; font-weight: 700;">{total_stops}</p>
                    <p style="margin: 5px 0 0 0; color: #1e40af; font-size: 14px;">Paradas totales</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 25px;">
            <tr>
                <td width="33%" style="padding: 15px; background-color: #dcfce7; border-radius: 8px; text-align: center; margin-right: 10px;">
                    <p style="margin: 0; color: #166534; font-size: 28px; font-weight: 700;">{completed_stops}</p>
                    <p style="margin: 5px 0 0 0; color: #166534; font-size: 13px;">Completadas</p>
                </td>
                <td width="5%"></td>
                <td width="33%" style="padding: 15px; background-color: #fee2e2; border-radius: 8px; text-align: center;">
                    <p style="margin: 0; color: #991b1b; font-size: 28px; font-weight: 700;">{failed_stops}</p>
                    <p style="margin: 5px 0 0 0; color: #991b1b; font-size: 13px;">Fallidas</p>
                </td>
                <td width="5%"></td>
                <td width="33%" style="padding: 15px; background-color: #e0e7ff; border-radius: 8px; text-align: center;">
                    <p style="margin: 0; color: #3730a3; font-size: 28px; font-weight: 700;">{success_rate}%</p>
                    <p style="margin: 5px 0 0 0; color: #3730a3; font-size: 13px;">Tasa de éxito</p>
                </td>
            </tr>
        </table>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es/dashboard" style="display: inline-block; background-color: #22c55e; color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 8px; font-weight: 600; font-size: 16px;">
                Ver detalles en Dashboard
            </a>
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": f"📊 Resumen de entregas - {date}",
            "html": get_base_template(content, f"Resumen {date}")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}
