from sqlalchemy.orm import Session
from .models import Notification, User
import datetime as dt

def create_notification(
    db: Session,
    user_id: int,
    title: str,
    message: str,
    category: str = "system",
    link: str | None = None
) -> Notification:
    """
    Crea una nueva notificación para un usuario.
    """
    notif = Notification(
        user_id=user_id,
        title=title,
        message=message,
        category=category,
        link=link,
        created_at=dt.datetime.utcnow(),
        is_read=False
    )
    db.add(notif)
    db.commit()
    db.refresh(notif)
    return notif

def get_unread_count(db: Session, user_id: int) -> int:
    """
    Devuelve la cantidad de notificaciones no leídas de un usuario.
    """
    return db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.is_read == False
    ).count()

def get_user_notifications(
    db: Session,
    user_id: int,
    skip: int = 0,
    limit: int = 20,
    only_unread: bool = False
):
    """
    Obtiene lista paginada de notificaciones.
    """
    q = db.query(Notification).filter(Notification.user_id == user_id)
    if only_unread:
        q = q.filter(Notification.is_read == False)
    
    return q.order_by(Notification.created_at.desc()).offset(skip).limit(limit).all()

def mark_as_read(db: Session, notification_id: int, user_id: int) -> bool:
    """
    Marca una notificación como leída. Retorna True si existía y era del usuario.
    """
    notif = db.query(Notification).filter(
        Notification.id == notification_id,
        Notification.user_id == user_id
    ).first()
    
    if not notif:
        return False
        
    notif.is_read = True
    db.commit()
    return True

def mark_all_as_read(db: Session, user_id: int):
    """
    Marca todas las notificaciones de un usuario como leídas.
    """
    db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.is_read == False
    ).update({Notification.is_read: True}, synchronize_session=False)
    db.commit()

def delete_notification(db: Session, notification_id: int, user_id: int) -> bool:
    """
    Elimina una notificación de un usuario. Retorna True si existía y se borró.
    """
    notif = db.query(Notification).filter(
        Notification.id == notification_id,
        Notification.user_id == user_id
    ).first()
    
    if not notif:
        return False
        
    db.delete(notif)
    db.commit()
    return True
