from datetime import datetime, timedelta
from pymongo import MongoClient
import random
import string

client = MongoClient('mongodb://admin:admin@mongodb:27017/')
db = client['db']

def random_string(length: int) -> str:
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz     ', k=length))

def random_date(start_date: datetime, end_date: datetime) -> datetime:
    time_between = end_date - start_date
    random_days = random.randint(0, time_between.days)
    return start_date + timedelta(days=random_days)

def generate_user_sessions(count: int) -> None:
    sessions = []
    devices = ['mobile', 'desktop', 'api']
    pages = ['/home', '/products', '/cart', '/checkout', '/profile']
    actions = ['login', 'view_product', 'add_to_cart', 'remove_from_cart', 'purchase', 'logout', 'search', 'filter']
    for i in range(count):
        start_time = datetime.now() - timedelta(days=random.randint(0, 30))
        end_time = start_time + timedelta(minutes=random.randint(1, 24 * 60))
        session = {
            'session_id': f'sess_{i:05d}',
            'user_id': f'user_{random.randint(1, 50):03d}',
            'start_time': start_time,
            'end_time': end_time,
            'device': random.choice(devices),
            'pages_visited': random.choices(pages, k=random.randint(1, 5)),
            'actions': random.choices(actions, k=random.randint(1, 10))
        }
        sessions.append(session)
    db.user_sessions.delete_many({})
    db.user_sessions.insert_many(sessions)

def generate_event_logs(count: int) -> None:
    events = []
    event_types = ['click', 'scroll', 'submit', 'error']
    pages = ['/home', '/products', '/cart', '/checkout', '/profile']
    for i in range(count):
        event = {
            'event_id': f'evt_{i:05d}',
            'timestamp': datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59)),
            'event_type': random.choice(event_types),
            'details': {
                'page': random.choice(pages),
                'element': f'button_{random.randint(1, 10)}',
                'user_id': f'user_{random.randint(1, 50):03d}'
            }
        }
        events.append(event)
    db.event_logs.delete_many({})
    db.event_logs.insert_many(events)

def generate_support_tickets(count: int) -> None:
    tickets = []
    issue_types = ['payment', 'delivery', 'product_quality', 'technical', 'refund', 'account']
    statuses = ['open', 'in_progress', 'closed', 'pending']
    for i in range(count):
        created_at = datetime.now() - timedelta(days=random.randint(0, 60))
        ticket = {
            'ticket_id': f'ticket_{i:05d}',
            'user_id': f'user_{random.randint(1, 50):03d}',
            'issue_type': random.choice(issue_types),
            'status': random.choice(statuses),
            'messages': [
                {
                    'sender': 'user' if j % 2 == 0 else 'support',
                    'message': random_string(random.randint(10, 50)),
                    'timestamp': created_at + timedelta(hours=j, minutes=random.randint(0, 59))
                }
                for j in range(random.randint(1, 5))
            ],
            'created_at': created_at,
            'updated_at': created_at + timedelta(hours=random.randint(1, 48))
        }
        tickets.append(ticket)
    db.support_tickets.delete_many({})
    db.support_tickets.insert_many(tickets)
    
def generate_user_recommendations(count: int) -> None:
    recommendations = []
    for i in range(1, count + 1):
        recommendation = {
            'user_id': f'user_{i:03d}',
            'recommended_products': [f'prod_{random.randint(1, 200):03d}' for _ in range(random.randint(3, 10))],
            'last_updated': datetime.now() - timedelta(days=random.randint(0, 7))
        }
        recommendations.append(recommendation)
    db.user_recommendations.delete_many({})
    db.user_recommendations.insert_many(recommendations)

def generate_moderation_queue(count: int) -> None:
    reviews = []
    statuses = ['pending', 'approved', 'rejected']
    flags = [['contains_images'], ['inadequate_client'], ['contains_images', 'inadequate_client'], []]
    for i in range(count):
        review = {
            'review_id': f'rev_{i:05d}',
            'product_id': f'prod_{random.randint(1, 100):03d}',
            'user_id': f'user_{random.randint(1, 50):03d}',
            'review_text': random.choice([
                'Хороший товар',
                'Плохой товар',
                'Хороший товар, рекомендую',
                'Плохой товар, продавец кидала',
                'Не соответствует описанию',
                'Норм за свои деньги',
                'Пришел дефектный товар',
                'Обман, не покупайте тут ничего'
            ]),
            'rating': random.randint(1, 5),
            'flags': random.choice(flags),
            'moderation_status': random.choice(statuses),
            'submitted_at': datetime.now() - timedelta(days=random.randint(0, 30))
        }
        reviews.append(review)
    db.moderation_queue.delete_many({})
    db.moderation_queue.insert_many(reviews)

def main():
    generate_user_sessions(100)
    generate_event_logs(200)
    generate_support_tickets(50)
    generate_user_recommendations(50)
    generate_moderation_queue(80)
    print('SUCCESS')

if __name__ == '__main__':
    main()
