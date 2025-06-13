import os
from cachelib.redis import RedisCache

# Cấu hình cơ sở dữ liệu
SQLALCHEMY_DATABASE_URI = f"postgresql://superset:superset@postgres:5432/superset"

# Cấu hình Redis cho caching
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}

# Cấu hình Celery cho truy vấn bất đồng bộ
class CeleryConfig:
    BROKER_URL = 'redis://redis:6379/0'
    CELERY_IMPORTS = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
        "superset.tasks.thumbnails",
        "superset.tasks.cache",
    )
    CELERY_RESULT_BACKEND = 'redis://redis:6379/0'
    CELERYD_LOG_LEVEL = 'DEBUG'
    CELERYD_PREFETCH_MULTIPLIER = 1
    CELERY_ACKS_LATE = False
    CELERY_ANNOTATIONS = {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
    }

CELERY_CONFIG = CeleryConfig

# Cờ tính năng
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'SQLLAB_BACKEND_PERSISTENCE': True,
    'ENABLE_EXPLORE_JSON_CSRF_PROTECTION': False,
    'ENABLE_EXPLORE_DRAG_AND_DROP': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'GLOBAL_ASYNC_QUERIES': False,
}

# Khóa bí mật
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY')
JWT_SECRET = os.environ.get('JWT_SECRET')

# Cấu hình SQL Lab
SQLLAB_CTAS_NO_LIMIT = True
SQLLAB_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 60 * 60 * 6  # 6 giờ

# Bật CORS cho phát triển
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Cấu hình bổ sung
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Giới hạn hàng cho truy vấn SQL
ROW_LIMIT = 5000

# Bật vai trò công khai
PUBLIC_ROLE_LIKE_GAMMA = True

# CSS tùy chỉnh
SUPERSET_WEBSERVER_TIMEOUT = 60

# Cấu hình ghi log
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'DEBUG'
DATA_DIR = '/app/superset_home'
FILENAME = os.path.join(DATA_DIR, 'superset.log')

# Cấu hình truy vấn bất đồng bộ
RESULTS_BACKEND = RedisCache(
    host='redis',
    port=6379,
    db=1,
    key_prefix='superset_results'
)

# Cấu hình cơ sở dữ liệu bổ sung
DATABASES = {
    'clickhouse': {
        'driver': 'clickhouse',
        'username': os.environ.get('CLICKHOUSE_USER', 'default'),
        'password': os.environ.get('CLICKHOUSE_PASSWORD', ''),
        'host': 'clickhouse-server',
        'port': 8123,
        'database': os.environ.get('CLICKHOUSE_DB', 'default')
    }
}