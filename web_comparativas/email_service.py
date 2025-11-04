# web_comparativas/email_service.py
from __future__ import annotations
import os, ssl, smtplib, threading, datetime as dt
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, List

from fastapi import BackgroundTasks
from .models import db_session as scoped_db, User, Upload as UploadModel
from .models import EmailNotification  # tabla con UNIQUE(upload_id,user_id,kind)

# ===== Config =====
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL", "http://127.0.0.1:8000").rstrip("/")
APP_LOGO_URL   = os.getenv("APP_LOGO_URL", f"{APP_PUBLIC_URL}/static/img/logo-suizo.png")
FROM_NAME      = os.getenv("MAIL_FROM_NAME", "Web Comparativas – Suizo Argentina")
FROM_EMAIL     = os.getenv("MAIL_FROM_EMAIL", "no-reply@suizo.com.ar")

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_TLS  = os.getenv("SMTP_TLS", "1") not in {"0", "false", "False", ""}

# 📧 Admin destinatarios (separar por comas si hay varios)
ADMIN_EMAILS = [
    e.strip() for e in os.getenv("ADMIN_EMAILS", "admin@suizo.com.ar").split(",") if e.strip()
]

# ===== Helpers =====
def _dashboard_url(upload_id: int) -> str:
    return f"{APP_PUBLIC_URL}/tablero/{upload_id}?autoredir=1"

def _build_subject(up: UploadModel) -> str:
    proc = (up.proceso_nro or str(up.id)).strip()
    return f"Proceso {proc} finalizado – Abrí tu tablero"

def _build_plain(up: UploadModel) -> str:
    proc = (up.proceso_nro or str(up.id)).strip()
    url  = _dashboard_url(up.id)
    lines = [
        f"¡Listo! El proceso {proc} está finalizado.",
        "",
        f"Abrir tablero: {url}",
        "",
        "Este mensaje se envía automáticamente. No responder."
    ]
    return "\n".join(lines)

def _build_html(up: UploadModel) -> str:
    proc = (up.proceso_nro or str(up.id)).strip()
    apertura = (up.apertura_fecha or "").strip()
    cuenta   = (up.cuenta_nro or "").strip()
    url      = _dashboard_url(up.id)
    return f"""\
<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#f6f7fb;font-family:Segoe UI,Arial,sans-serif;color:#0f172a;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f6f7fb;padding:24px 0;">
      <tr>
        <td align="center">
          <table role="presentation" width="600" cellspacing="0" cellpadding="0" style="background:#ffffff;border-radius:12px;box-shadow:0 5px 24px rgba(6,64,102,.06);overflow:hidden;">
            <tr>
              <td style="background:#064066;padding:16px 24px;">
                <img src="{APP_LOGO_URL}" alt="Suizo Argentina" style="display:block;height:32px;">
              </td>
            </tr>
            <tr>
              <td style="padding:28px 24px 8px 24px;">
                <h1 style="margin:0 0 12px 0;font-size:20px;line-height:1.3;color:#064066;">Tu proceso está listo</h1>
                <p style="margin:0 0 4px 0;font-size:14px;">Proceso: <strong>{proc}</strong></p>
                {"<p style='margin:0 0 4px 0;font-size:14px;'>Apertura: <strong>"+apertura+"</strong></p>" if apertura else ""}
                {"<p style='margin:0 0 4px 0;font-size:14px;'>Cuenta: <strong>"+cuenta+"</strong></p>" if cuenta else ""}
                <p style="margin:12px 0 0 0;font-size:14px;">El tablero ya se puede abrir. Hacé clic acá:</p>
              </td>
            </tr>
            <tr>
              <td style="padding:8px 24px 24px 24px;">
                <table role="presentation" cellspacing="0" cellpadding="0">
                  <tr>
                    <td>
                      <a href="{url}"
                         style="display:inline-block;background:#5274ce;color:#ffffff;text-decoration:none;
                                padding:12px 18px;border-radius:10px;font-weight:600;font-size:14px;">
                        Abrir tablero
                      </a>
                    </td>
                  </tr>
                </table>
                <p style="margin:18px 0 0 0;font-size:12px;color:#6b7280;">
                  Si el botón no funciona, copiá y pegá este enlace en tu navegador:<br>
                  <a href="{url}" style="color:#5274ce;text-decoration:underline;">{url}</a>
                </p>
              </td>
            </tr>
            <tr>
              <td style="background:#f3f5f9;padding:14px 24px;font-size:12px;color:#475569;">
                Este mensaje se envió automáticamente desde Web Comparativas – Suizo Argentina.
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()

# === Base SMTP ===
def _send_mail(to_addrs: List[str], subject: str, html: str, plain: str) -> None:
    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST no configurado")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = formataddr((FROM_NAME, FROM_EMAIL))
    msg["To"]      = ", ".join(to_addrs)

    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        if SMTP_TLS:
            server.starttls(context=context)
        if SMTP_USER:
            server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, to_addrs, msg.as_string())

# === Mail “proceso finalizado” ===
def _recipients_for(up: UploadModel) -> List[str]:
    u = scoped_db.get(User, up.user_id) if up.user_id else None
    if u and u.email:
        return [u.email.strip()]
    return []

def _send_and_register(upload_id: int, user_id: int, subject: str, html: str, plain: str):
    session = scoped_db
    try:
        rec = EmailNotification(
            upload_id=upload_id,
            user_id=user_id,
            kind="done",
            subject=subject,
            status="pending",
            created_at=dt.datetime.utcnow(),
        )
        session.add(rec)
        session.commit()
    except Exception:
        session.rollback()
        return

    try:
        _send_mail(_recipients_for(session.get(UploadModel, upload_id)), subject, html, plain)
        rec.status = "sent"
        rec.sent_at = dt.datetime.utcnow()
        session.commit()
    except Exception as e:
        session.rollback()
        try:
            rec.status = "error"
            rec.error = str(e)
            rec.updated_at = dt.datetime.utcnow()
            session.commit()
        except Exception:
            session.rollback()

def maybe_send_done_email(session, up: UploadModel, *, background_tasks: Optional[BackgroundTasks] = None) -> bool:
    recips = _recipients_for(up)
    if not recips:
        return False
    user = session.get(User, up.user_id)
    if user is None or not user.email:
        return False

    already = (
        session.query(EmailNotification)
        .filter(
            EmailNotification.upload_id == up.id,
            EmailNotification.user_id == user.id,
            EmailNotification.kind == "done",
        )
        .first()
    )
    if already:
        return False

    subject = _build_subject(up)
    html    = _build_html(up)
    plain   = _build_plain(up)

    if background_tasks is not None:
        background_tasks.add_task(_send_and_register, up.id, user.id, subject, html, plain)
    else:
        threading.Thread(target=_send_and_register, args=(up.id, user.id, subject, html, plain), daemon=True).start()
    return True


# =====================================================================
# 📢 NUEVO: Notificación de errores al admin
# =====================================================================
def send_admin_alert_email(upload: Optional[UploadModel], error_msg: str):
    """
    Envía un correo al admin cuando ocurre un error grave en la normalización o procesamiento.
    """
    if not ADMIN_EMAILS:
        return False

    proc = (upload.proceso_nro if upload else "Desconocido")
    user_email = ""
    if upload and upload.user_id:
        user = scoped_db.get(User, upload.user_id)
        user_email = f"{user.email}" if user and user.email else "Desconocido"

    subject = f"⚠️ Error en proceso {proc or upload.id} – Web Comparativas"
    plain = f"""Se detectó un error en el proceso {proc}:

{error_msg}

Usuario: {user_email or 'Desconocido'}
Fecha: {dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

Este mensaje fue generado automáticamente por Web Comparativas.
"""

    html = f"""\
<!doctype html>
<html>
  <body style="background:#f6f7fb;padding:20px;font-family:Segoe UI,Arial,sans-serif;color:#0f172a;">
    <table width="100%" cellspacing="0" cellpadding="0" style="max-width:650px;margin:auto;background:#fff;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.08);">
      <tr><td style="background:#8b0000;color:#fff;padding:14px 20px;font-size:18px;font-weight:600;">
        ⚠️ Error en proceso {proc or upload.id}
      </td></tr>
      <tr><td style="padding:20px;font-size:14px;line-height:1.5;">
        <p><strong>Detalle del error:</strong></p>
        <pre style="background:#f3f4f6;padding:12px;border-radius:6px;white-space:pre-wrap;">{error_msg}</pre>
        <p><strong>Usuario:</strong> {user_email or 'Desconocido'}</p>
        <p><strong>Fecha:</strong> {dt.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        <p style="margin-top:20px;">Podés revisar el proceso desde el panel de administración.</p>
      </td></tr>
      <tr><td style="background:#f3f5f9;padding:12px 20px;font-size:12px;color:#475569;">
        Este mensaje fue generado automáticamente por Web Comparativas – Suizo Argentina.
      </td></tr>
    </table>
  </body>
</html>
"""

    # Ejecutar en background
    threading.Thread(
        target=_send_mail,
        args=(ADMIN_EMAILS, subject, html, plain),
        daemon=True,
    ).start()

    return True
