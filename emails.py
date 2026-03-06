"""
Xpedit - Servicio de emails con Resend
"""

import os
from html import escape as html_escape
from typing import List, Optional

import resend

# Configurar API key
resend.api_key = os.getenv("RESEND_API_KEY")

# Email de envío (dominio verificado)
FROM_EMAIL = "Xpedit <info@xpedit.es>"
REPLY_TO = "info@xpedit.es"

# WhatsApp contact
WHATSAPP_URL = "https://wa.me/34632073689"

# App store links
PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=com.taespack.rutamax"
APP_STORE_URL = "https://apps.apple.com/app/xpedit-planificador-de-rutas/id6740513547"


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
    <body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f0f4f8;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f0f4f8; padding: 40px 20px;">
            <tr>
                <td align="center">
                    <table width="100%" style="max-width: 600px; background-color: #ffffff; border-radius: 16px; overflow: hidden; box-shadow: 0 4px 24px rgba(0, 0, 0, 0.08);">
                        <!-- Header -->
                        <tr>
                            <td style="background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); padding: 32px; text-align: center;">
                                <h1 style="margin: 0; color: #ffffff; font-size: 28px; font-weight: 700; letter-spacing: -0.5px;">Xpedit</h1>
                                <p style="margin: 8px 0 0 0; color: rgba(255,255,255,0.85); font-size: 14px;">Optimiza tus rutas de reparto</p>
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
                            <td style="background-color: #f8fafc; padding: 25px 30px; text-align: center; border-top: 1px solid #e2e8f0;">
                                <p style="margin: 0 0 12px 0; color: #64748b; font-size: 13px;">
                                    Este email fue enviado por Xpedit
                                </p>
                                <p style="margin: 0; color: #94a3b8; font-size: 12px;">
                                    <a href="https://xpedit.es" style="color: #3b82f6; text-decoration: none;">xpedit.es</a>
                                    &nbsp;&middot;&nbsp;
                                    <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none;">WhatsApp</a>
                                    &nbsp;&middot;&nbsp;
                                    <a href="https://xpedit.es/legal/privacidad" style="color: #3b82f6; text-decoration: none;">Privacidad</a>
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
    """Email de bienvenida para nuevos usuarios - guía de activación en 3 pasos"""
    user_name = html_escape(user_name)
    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px;">Hola {user_name}, tu primera ruta te espera</h2>

        <p style="margin: 0 0 25px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Crear y optimizar una ruta con Xpedit lleva menos de 2 minutos. Así de fácil:
        </p>

        <!-- Paso 1 -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 18px;">
            <tr>
                <td width="50" valign="top" style="padding-right: 14px;">
                    <div style="background-color: #3b82f6; color: #fff; width: 36px; height: 36px; border-radius: 50%; text-align: center; line-height: 36px; font-weight: 700; font-size: 18px;">1</div>
                </td>
                <td valign="top">
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 16px; font-weight: 600;">Abre la app y agrega paradas</p>
                    <p style="margin: 0; color: #6b7280; font-size: 14px;">Escribe direcciones, usa la voz o escanea etiquetas con la cámara.</p>
                </td>
            </tr>
        </table>

        <!-- Paso 2 -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 18px;">
            <tr>
                <td width="50" valign="top" style="padding-right: 14px;">
                    <div style="background-color: #10b981; color: #fff; width: 36px; height: 36px; border-radius: 50%; text-align: center; line-height: 36px; font-weight: 700; font-size: 18px;">2</div>
                </td>
                <td valign="top">
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 16px; font-weight: 600;">Pulsa "Optimizar"</p>
                    <p style="margin: 0; color: #6b7280; font-size: 14px;">Nuestra IA ordena las paradas en el recorrido más corto. Ahorra hasta un 30% en km.</p>
                </td>
            </tr>
        </table>

        <!-- Paso 3 -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 25px;">
            <tr>
                <td width="50" valign="top" style="padding-right: 14px;">
                    <div style="background-color: #f59e0b; color: #fff; width: 36px; height: 36px; border-radius: 50%; text-align: center; line-height: 36px; font-weight: 700; font-size: 18px;">3</div>
                </td>
                <td valign="top">
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 16px; font-weight: 600;">Navega y entrega</p>
                    <p style="margin: 0; color: #6b7280; font-size: 14px;">Navegación paso a paso con voz. Marca entregas con foto y firma.</p>
                </td>
            </tr>
        </table>

        <!-- App screenshot -->
        <div style="text-align: center; margin: 25px 0;">
            <img src="https://xpedit.es/screenshots/route-optimized.png" alt="Ruta optimizada en Xpedit" style="max-width: 280px; width: 100%; border-radius: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.12);">
        </div>

        <!-- Trial callout -->
        <div style="background: linear-gradient(135deg, #eff6ff 0%, #f0fdf4 100%); border-radius: 12px; padding: 20px; margin: 25px 0; text-align: center; border: 1px solid #e0e7ff;">
            <p style="margin: 0 0 6px 0; color: #1e40af; font-size: 14px; font-weight: 600;">REGALO DE BIENVENIDA</p>
            <p style="margin: 0 0 4px 0; color: #111827; font-size: 22px; font-weight: 700;">14 días de Pro+ gratis</p>
            <p style="margin: 0; color: #6b7280; font-size: 14px;">Paradas ilimitadas, optimización avanzada y más.</p>
        </div>

        <!-- LATAM mention -->
        <div style="text-align: center; margin: 20px 0;">
            <p style="margin: 0; color: #64748b; font-size: 13px;">
                Disponible en España y toda Latinoamérica
            </p>
        </div>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es" style="display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);">
                Abrir Xpedit
            </a>
        </div>

        <p style="margin: 25px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            ¿Necesitas ayuda? <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none; font-weight: 500;">Escríbenos por WhatsApp</a> o responde a este email.
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": "Crea tu primera ruta en 2 minutos",
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
    client_name = html_escape(client_name)
    driver_name = html_escape(driver_name)
    estimated_time = html_escape(estimated_time) if estimated_time else None
    tracking_url = html_escape(tracking_url) if tracking_url else None

    time_text = f"<p style='margin: 0 0 20px 0; color: #4b5563; font-size: 16px;'>Tiempo estimado de llegada: <strong>{estimated_time}</strong></p>" if estimated_time else ""

    tracking_button = ""
    if tracking_url:
        tracking_button = f"""
        <div style="text-align: center; margin: 30px 0;">
            <a href="{tracking_url}" style="display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);">
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
    client_name = html_escape(client_name)
    delivery_time = html_escape(delivery_time)
    photo_url = html_escape(photo_url) if photo_url else None
    recipient_name = html_escape(recipient_name) if recipient_name else None

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

        <div style="background-color: #f8fafc; border-radius: 8px; padding: 20px; margin: 20px 0;">
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
    client_name = html_escape(client_name)
    reason = html_escape(reason) if reason else None
    next_attempt = html_escape(next_attempt) if next_attempt else None

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
                <strong>¿Necesitas ayuda?</strong> Contáctanos en <a href="{WHATSAPP_URL}" style="color: #16a34a; text-decoration: none;">WhatsApp</a> o escríbenos a info@xpedit.es
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
    dispatcher_name = html_escape(dispatcher_name)
    date = html_escape(date)

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
            <a href="https://xpedit.es/dashboard" style="display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);">
                Ver detalles en Dashboard
            </a>
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": f"Resumen de entregas - {date}",
            "html": get_base_template(content, f"Resumen {date}")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_plan_activated_email(to_email: str, user_name: str, plan_name: str, days: Optional[int] = None, permanent: bool = False) -> dict:
    """Email cuando se activa un plan (por admin o por compra)"""
    user_name = html_escape(user_name)
    plan_name = html_escape(plan_name)
    duration_text = "de forma permanente" if permanent else f"durante {days} días"

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #eff6ff; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">{"👑" if "Plus" in plan_name or "plus" in plan_name else "⭐"}</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            ¡Plan {plan_name} activado!
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{user_name}</strong>,
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Tu plan <strong>{plan_name}</strong> ha sido activado {duration_text}. Ya puedes disfrutar de todas las ventajas.
        </p>

        <div style="background-color: #f0fdf4; border-radius: 12px; padding: 20px; margin: 25px 0;">
            <h3 style="margin: 0 0 15px 0; color: #166534; font-size: 16px;">Tus beneficios:</h3>
            <ul style="margin: 0; padding-left: 20px; color: #166534; font-size: 14px; line-height: 2;">
                <li>Más paradas diarias</li>
                <li>Optimización de rutas avanzada</li>
                <li>Soporte prioritario</li>
            </ul>
        </div>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es" style="display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);">
                Abrir Xpedit
            </a>
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": f"Plan {plan_name} activado",
            "html": get_base_template(content, f"Plan {plan_name}")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_referral_reward_email(to_email: str, user_name: str, referred_name: str, reward_days: int) -> dict:
    """Email cuando un referido se registra y ambos reciben reward"""
    user_name = html_escape(user_name)
    referred_name = html_escape(referred_name)
    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #fef3c7; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">🎉</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            ¡Has ganado {reward_days} días Pro gratis!
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{user_name}</strong>,
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            <strong>{referred_name}</strong> se ha registrado con tu código de invitación. Como agradecimiento, ambos recibís <strong>{reward_days} días de Pro gratis</strong>.
        </p>

        <div style="background-color: #eff6ff; border-radius: 12px; padding: 20px; margin: 25px 0; text-align: center;">
            <p style="margin: 0 0 5px 0; color: #1e40af; font-size: 14px;">Tu recompensa</p>
            <p style="margin: 0; color: #1e40af; font-size: 28px; font-weight: 700;">{reward_days} días Pro</p>
        </div>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Sigue invitando amigos para acumular más días gratis.
        </p>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es" style="display: inline-block; background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(245, 158, 11, 0.3);">
                Invitar más amigos
            </a>
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": f"🎉 Has ganado {reward_days} días Pro gratis",
            "html": get_base_template(content, "Recompensa referido")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_upcoming_email(
    to_email: str,
    client_name: str,
    driver_name: str,
    stops_away: int,
    tracking_url: Optional[str] = None
) -> dict:
    """Email cuando el repartidor está a X paradas del cliente"""
    client_name = html_escape(client_name)
    driver_name = html_escape(driver_name)
    tracking_url = html_escape(tracking_url) if tracking_url else None

    tracking_button = ""
    if tracking_url:
        tracking_button = f"""
        <div style="text-align: center; margin: 30px 0;">
            <a href="{tracking_url}" style="display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);">
                Seguir mi pedido en vivo
            </a>
        </div>
        """

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #eff6ff; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">📦</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            Tu pedido llega pronto
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola{f' <strong>{client_name}</strong>' if client_name else ''},
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Tu repartidor <strong>{driver_name}</strong> está a <strong>{stops_away} paradas</strong> de llegar a tu dirección.
        </p>

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
            "subject": f"📦 Tu pedido está a {stops_away} paradas",
            "html": get_base_template(content, "Pedido en camino")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_password_reset_email(to_email: str, user_name: str, new_password: str) -> dict:
    """Email con la nueva contraseña temporal cuando admin resetea"""
    user_name = html_escape(user_name or "usuario")
    new_password = html_escape(new_password)

    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            Tu contraseña ha sido restablecida
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{user_name}</strong>,
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Un administrador ha restablecido tu contraseña de Xpedit. Tu nueva contraseña temporal es:
        </p>

        <div style="background-color: #f0fdf4; border: 2px solid #22c55e; border-radius: 12px; padding: 20px; margin: 25px 0; text-align: center;">
            <p style="margin: 0 0 6px 0; color: #166534; font-size: 14px; font-weight: 600;">NUEVA CONTRASEÑA</p>
            <p style="margin: 0; color: #111827; font-size: 24px; font-weight: 700; font-family: monospace; letter-spacing: 2px;">{new_password}</p>
        </div>

        <div style="background-color: #fef3c7; border-radius: 8px; padding: 15px; margin: 20px 0;">
            <p style="margin: 0; color: #92400e; font-size: 14px;">
                <strong>Importante:</strong> Cambia esta contraseña la próxima vez que inicies sesión.
            </p>
        </div>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es" style="display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);">
                Iniciar sesión
            </a>
        </div>

        <p style="margin: 25px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            Si no solicitaste este cambio, contáctanos en <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none;">WhatsApp</a> o en info@xpedit.es
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": "Tu nueva contraseña de Xpedit",
            "html": get_base_template(content, "Nueva contraseña")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_custom_email(to_email: str, subject: str, body_html: str) -> dict:
    """Email personalizado desde admin"""
    content = f"""
        <div style="color: #4b5563; font-size: 16px; line-height: 1.6;">
            {body_html}
        </div>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": subject,
            "html": get_base_template(content, subject)
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_alert_email(to_email: str, alert_title: str, details: str) -> dict:
    """Email de alerta del sistema (health check, errores críticos)"""
    alert_title = html_escape(alert_title)
    details = html_escape(details)
    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 24px; text-align: center;">
            {alert_title}
        </h2>

        <div style="background-color: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 20px; margin: 20px 0;">
            <pre style="margin: 0; color: #991b1b; font-size: 13px; white-space: pre-wrap; word-break: break-all; font-family: monospace;">{details}</pre>
        </div>

        <p style="margin: 20px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            Revisa el estado en <a href="https://xpedit.es/api/health" style="color: #dc2626;">xpedit.es/api/health</a>
        </p>
    """

    alert_template = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #f4f4f5;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f4f4f5; padding: 40px 20px;">
            <tr><td align="center">
                <table width="100%" style="max-width: 600px; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                    <tr><td style="background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%); padding: 30px; text-align: center;">
                        <h1 style="margin: 0; color: #fff; font-size: 28px;">Xpedit ALERTA</h1>
                    </td></tr>
                    <tr><td style="padding: 40px 30px;">{content}</td></tr>
                </table>
            </td></tr>
        </table>
    </body>
    </html>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": f"[ALERTA] {alert_title}",
            "html": alert_template,
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_broadcast_email(to_emails: List[str], subject: str, body_html: str) -> dict:
    """Email masivo a múltiples usuarios"""
    results = {"sent": 0, "failed": 0, "errors": []}

    for email in to_emails:
        result = send_custom_email(email, subject, body_html)
        if result["success"]:
            results["sent"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({"email": email, "error": result.get("error", "unknown")})

    return results


def send_reengagement_email(to_email: str, user_name: str) -> dict:
    """Email de re-engagement para traer de vuelta usuarios inactivos"""
    user_name = html_escape(user_name or "repartidor")
    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            ¡Hemos mejorado Xpedit!
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{user_name}</strong>,
        </p>

        <p style="margin: 0 0 25px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hace tiempo que no te pasas por Xpedit y queríamos contarte todo lo que ha cambiado. Hemos estado trabajando duro para que planificar tus rutas sea aún más fácil y fiable:
        </p>

        <!-- Novedades -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">🔍</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Búsqueda de direcciones mejorada</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Autocompletado más rápido y preciso. Encuentra cualquier dirección al instante, incluso en zonas rurales.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">🚀</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Optimización más rápida y precisa</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Nuevo algoritmo con distancias reales por carretera. Rutas hasta un 30% más cortas.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">📍</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">20 paradas/día gratis</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Hemos ampliado el plan gratuito: optimiza hasta 20 paradas al día sin coste.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">📸</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Prueba de entrega con foto y firma</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Captura fotos y firmas como comprobante de cada entrega. Todo queda registrado.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">⚡</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Mayor estabilidad y rendimiento</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">App más ligera, arranque más rápido y navegación sin cortes. Hemos corregido decenas de errores.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 20px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">🎤</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Asistente de voz <span style="background-color: #dbeafe; color: #1e40af; font-size: 11px; font-weight: 700; padding: 2px 6px; border-radius: 4px; margin-left: 6px;">BETA</span></p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Di "Oye copiloto" y controla la app sin manos mientras conduces. Función en fase beta.</p>
                </td>
            </tr>
        </table>

        <!-- App screenshot -->
        <div style="text-align: center; margin: 25px 0;">
            <img src="https://xpedit.es/screenshots/route-optimized.png" alt="Ruta optimizada en Xpedit" style="max-width: 280px; width: 100%; border-radius: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.12);">
        </div>

        <!-- LATAM mention -->
        <div style="background: linear-gradient(135deg, #eff6ff 0%, #f0fdf4 100%); border-radius: 12px; padding: 20px; margin: 20px 0; text-align: center; border: 1px solid #e0e7ff;">
            <p style="margin: 0 0 6px 0; color: #1e40af; font-size: 15px; font-weight: 600;">
                Disponible en España y toda Latinoamérica
            </p>
            <p style="margin: 0; color: #64748b; font-size: 13px;">
                México, Colombia, Argentina, Chile, Perú y más. Misma app, misma calidad, estés donde estés.
            </p>
        </div>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es" style="display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);">
                Vuelve a probar Xpedit
            </a>
        </div>

        <p style="margin: 20px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            ¿Tienes dudas? <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none; font-weight: 500;">Escríbenos por WhatsApp</a> — te respondemos en minutos.
        </p>

        <p style="margin: 15px 0 0 0; color: #9ca3af; font-size: 12px; text-align: center;">
            Recibes este email porque tienes una cuenta en Xpedit. Si no quieres recibir más emails, responde con "cancelar".
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": "¡Hemos mejorado Xpedit! Mira las novedades",
            "html": get_base_template(content, "Novedades de Xpedit")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_reengagement_broadcast(to_emails_with_names: List[dict]) -> dict:
    """Envía email de re-engagement a múltiples usuarios. Cada item es {email, name}."""
    results = {"sent": 0, "failed": 0, "errors": []}

    for item in to_emails_with_names:
        result = send_reengagement_email(item["email"], item.get("name", ""))
        if result["success"]:
            results["sent"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({"email": item["email"], "error": result.get("error", "unknown")})

    return results


def send_social_login_announcement(to_email: str, user_name: str) -> dict:
    """Email anunciando social login (Google + Apple) a usuarios existentes"""
    user_name = html_escape(user_name or "repartidor")
    content = f"""
        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            Nuevo: inicia sesion con Google o Apple
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola <strong>{user_name}</strong>,
        </p>

        <p style="margin: 0 0 25px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Ahora puedes iniciar sesion en Xpedit con tu cuenta de Google o Apple. Sin recordar contrasenas, sin formularios. Un toque y estas dentro.
        </p>

        <!-- Features -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">&#127758;</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Google Login</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Inicia sesion con tu cuenta de Google en un toque. Funciona en la app y en la web.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">&#63743;</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Apple Login</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Si usas iPhone, inicia sesion con Face ID o Touch ID. Rapido, seguro y privado.</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 12px;">
            <tr>
                <td width="40" valign="top" style="padding-top: 2px;">
                    <span style="font-size: 20px;">&#128274;</span>
                </td>
                <td>
                    <p style="margin: 0 0 4px 0; color: #111827; font-size: 15px; font-weight: 600;">Tu cuenta no cambia</p>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 14px;">Tus rutas, paradas e historial siguen igual. Solo cambia como entras — todo lo demas se mantiene.</p>
                </td>
            </tr>
        </table>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://www.xpedit.es/login" style="display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 10px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);">
                Probar Social Login
            </a>
        </div>

        <p style="margin: 20px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            &iquest;Dudas? <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none; font-weight: 500;">Escribenos por WhatsApp</a> — te respondemos en minutos.
        </p>

        <p style="margin: 15px 0 0 0; color: #9ca3af; font-size: 12px; text-align: center;">
            Recibes este email porque tienes una cuenta en Xpedit. Si no quieres recibir mas emails, responde con &quot;cancelar&quot;.
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": "Nuevo: inicia sesion con Google o Apple",
            "html": get_base_template(content, "Social Login en Xpedit")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_social_login_broadcast(to_emails_with_names: List[dict]) -> dict:
    """Envia el anuncio de social login a multiples usuarios."""
    results = {"sent": 0, "failed": 0, "errors": []}

    for item in to_emails_with_names:
        result = send_social_login_announcement(item["email"], item.get("name", ""))
        if result["success"]:
            results["sent"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({"email": item["email"], "error": result.get("error", "unknown")})

    return results


def send_trial_expiring_email(to_email: str, user_name: str, plan_name: str, days_left: int) -> dict:
    """Email cuando el trial de un usuario está a punto de expirar (3 días antes)."""
    user_name = html_escape(user_name or "")
    plan_name = html_escape(plan_name)

    urgency_text = f"en {days_left} días" if days_left > 1 else "mañana" if days_left == 1 else "hoy"
    plan_display = "Pro+" if "plus" in plan_name.lower() else "Pro"
    price = "9,99€" if "plus" in plan_name.lower() else "4,99€"

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #fef3c7; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">&#9200;</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            Tu prueba {plan_display} termina {urgency_text}
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola{(' <strong>' + user_name + '</strong>') if user_name else ''},
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Tu periodo de prueba de <strong>Xpedit {plan_display}</strong> termina {urgency_text}.
            Cuando expire, perder&aacute;s acceso a las funciones avanzadas.
        </p>

        <div style="background-color: #fef2f2; border-radius: 12px; padding: 20px; margin: 25px 0; border: 1px solid #fecaca;">
            <h3 style="margin: 0 0 12px 0; color: #991b1b; font-size: 15px;">Lo que perder&aacute;s:</h3>
            <ul style="margin: 0; padding-left: 20px; color: #991b1b; font-size: 14px; line-height: 2;">
                <li>Optimizaci&oacute;n de rutas con IA (ahorra hasta 30% en km)</li>
                <li>Paradas ilimitadas por ruta</li>
                <li>Prueba de entrega con foto y firma</li>
                <li>Asistente de voz (beta)</li>
            </ul>
        </div>

        <div style="background-color: #eff6ff; border-radius: 12px; padding: 20px; margin: 25px 0; text-align: center; border: 1px solid #bfdbfe;">
            <p style="margin: 0 0 5px 0; color: #1e40af; font-size: 14px;">Contin&uacute;a con {plan_display} por solo</p>
            <p style="margin: 0; color: #1e40af; font-size: 32px; font-weight: 700;">{price}<span style="font-size: 16px; font-weight: 400;">/mes</span></p>
            <p style="margin: 8px 0 0 0; color: #3b82f6; font-size: 13px;">Cancela cuando quieras. Sin compromisos.</p>
        </div>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es/#pricing" style="display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); color: #ffffff; text-decoration: none; padding: 16px 40px; border-radius: 10px; font-weight: 600; font-size: 18px; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);">
                Suscribirme a {plan_display}
            </a>
        </div>

        <p style="margin: 20px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            &iquest;Dudas? <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none; font-weight: 500;">Escr&iacute;benos por WhatsApp</a> &mdash; te respondemos en minutos.
        </p>

        <p style="margin: 15px 0 0 0; color: #9ca3af; font-size: 12px; text-align: center;">
            Recibes este email porque tienes una cuenta en Xpedit.
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": f"Tu prueba {plan_display} termina {urgency_text}",
            "html": get_base_template(content, f"Trial {plan_display} expira")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_trial_expired_email(to_email: str, user_name: str, plan_name: str) -> dict:
    """Email cuando el trial ha expirado y el usuario ha sido degradado a Free."""
    user_name = html_escape(user_name or "")
    plan_display = "Pro+" if "plus" in plan_name.lower() else "Pro"
    price = "9,99€" if "plus" in plan_name.lower() else "4,99€"

    content = f"""
        <div style="text-align: center; margin-bottom: 25px;">
            <div style="display: inline-block; background-color: #fee2e2; border-radius: 50%; padding: 20px;">
                <span style="font-size: 40px;">&#128274;</span>
            </div>
        </div>

        <h2 style="margin: 0 0 20px 0; color: #111827; font-size: 24px; text-align: center;">
            Tu prueba {plan_display} ha terminado
        </h2>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Hola{(' <strong>' + user_name + '</strong>') if user_name else ''},
        </p>

        <p style="margin: 0 0 20px 0; color: #4b5563; font-size: 16px; line-height: 1.6;">
            Tu periodo de prueba de <strong>Xpedit {plan_display}</strong> ha terminado.
            Tu cuenta ha vuelto al plan <strong>Gratis</strong> con funciones limitadas.
        </p>

        <div style="background-color: #f0fdf4; border-radius: 12px; padding: 20px; margin: 25px 0; border: 1px solid #bbf7d0;">
            <h3 style="margin: 0 0 12px 0; color: #166534; font-size: 15px;">Recupera tus funciones {plan_display}:</h3>
            <ul style="margin: 0; padding-left: 20px; color: #166534; font-size: 14px; line-height: 2;">
                <li>Optimizaci&oacute;n IA &mdash; ahorra hasta 30% en km y combustible</li>
                <li>Paradas ilimitadas por ruta</li>
                <li>Prueba de entrega con foto y firma</li>
                <li>Soporte prioritario</li>
            </ul>
            <p style="margin: 15px 0 0 0; color: #166534; font-size: 16px; font-weight: 600; text-align: center;">
                Solo {price}/mes &mdash; cancela cuando quieras
            </p>
        </div>

        <div style="text-align: center; margin: 30px 0;">
            <a href="https://xpedit.es/#pricing" style="display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); color: #ffffff; text-decoration: none; padding: 16px 40px; border-radius: 10px; font-weight: 600; font-size: 18px; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);">
                Suscribirme ahora
            </a>
        </div>

        <p style="margin: 20px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            Mientras tanto, puedes seguir usando el plan Gratis con hasta 20 paradas por ruta.
        </p>

        <p style="margin: 15px 0 0 0; color: #6b7280; font-size: 14px; text-align: center;">
            &iquest;Dudas? <a href="{WHATSAPP_URL}" style="color: #22c55e; text-decoration: none; font-weight: 500;">Escr&iacute;benos por WhatsApp</a>
        </p>

        <p style="margin: 15px 0 0 0; color: #9ca3af; font-size: 12px; text-align: center;">
            Recibes este email porque tienes una cuenta en Xpedit.
        </p>
    """

    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "reply_to": REPLY_TO,
            "subject": f"Tu prueba {plan_display} ha terminado — suscríbete desde {price}/mes",
            "html": get_base_template(content, f"Trial {plan_display} expirado")
        })
        return {"success": True, "id": response["id"]}
    except Exception as e:
        return {"success": False, "error": str(e)}
