import pandas as pd
from random import randint, random, choice
import uuid

def generate_item():
    return {
        'id': str(uuid.uuid4()),
        'price': 10 ** (random() * 5),
        'amount': randint(1, 20),
        'category': choice(
            [
                'food',
                'medicine',
                'entertainment',
                'transport',
                'other',
                'supermarket',
                'travel',
                'services'
            ]
        ),
        'cashback_pct': choice(
            [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 5, 10]
        )
    }


def generate_data():
    df = pd.DataFrame()
    items = [generate_item() for i in range(700000)]
    for column_name in list(items[0].keys()):
        df[column_name] = [item[column_name] for item in items]
    df.to_json('cashbacks_dataset.csv')


if __name__ == '__main__':
    generate_data()
