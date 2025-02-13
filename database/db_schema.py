
DB_SCHEMA = {
    'objects': {
        'table_name': 'objects',
        'columns': {
            "id": "BIGINT PRIMARY KEY",
            "category": "VARCHAR(64)",
            "type": "VARCHAR(64)",
            "title": "VARCHAR(256)",
            "price": "TEXT",
            "price_for": "VARCHAR(64)",
            "location": "VARCHAR(256)",
            "photo_URLs": "TEXT[]",
            "source_URL": "VARCHAR(256)",
            "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            'user_id': 'BIGINT'
        },
        'unique_constraints': ['source_URL'],
        'indexes': [('user_id', 'idx_user_id'),('category', 'idx_category')]
    },

    'unique_records': {
        'table_name': 'unique_records',
        'columns': {
            "id": "BIGINT PRIMARY KEY",
            "category": "VARCHAR(64)",
            "type": "VARCHAR(64)",
            "title": "VARCHAR(256)",
            "price": "TEXT",
            "price_for": "VARCHAR(64)",
            "location": "VARCHAR(256)",
            "photo_URLs": "TEXT[]",
            "source_URL": "VARCHAR(256) UNIQUE",
            "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
        'unique_constraints': ['source_URL'],
        'foreign_keys': [],
        'indexes': [('category','idx_category')]
    },

    'users': {
        'table_name': 'users',
        'columns': {
            "id": "BIGSERIAL PRIMARY KEY",
            "username": "VARCHAR(32) UNIQUE",
            "phone_number": "VARCHAR(32) UNIQUE",
            "email": "VARCHAR(64)",
            "first_name": "VARCHAR(255)",
            "last_name": "VARCHAR(255)",
            "income_level": "VARCHAR(255)",
            "address": "VARCHAR(255)",
            "gender": "CHAR(2)",
            "property": "BIGINT",
            "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        },
        'unique_constraints': ['username', 'phone_number'],
        'foreign_keys': [('property', 'objects', 'id')],
        'indexes': []
    }
}