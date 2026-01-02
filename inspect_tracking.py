from web_comparativas.models import db_session, UsageEvent, User
s = db_session()
print("Total events:", s.query(UsageEvent).count())
last_events = s.query(UsageEvent).order_by(UsageEvent.timestamp.desc()).limit(5).all()
for ev in last_events:
    print(f"User: {ev.user_id} Role: {ev.user_role} Action: {ev.action_type} Section: {ev.section}")

u = s.query(User).filter(User.name == "Andres").first()
if u:
    print(f"User 'Andres' Role: {u.role}")
