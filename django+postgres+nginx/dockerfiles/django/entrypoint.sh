#!/bin/sh

cd /app/src

# Run migrations if database has no migrations table
if ! python manage.py showmigrations &>/dev/null; then
    python manage.py makemigrations --noinput
    python manage.py migrate --noinput
fi

# Always collect static (fast if nothing changed)
python manage.py collectstatic --noinput

gunicorn setting.wsgi:application --bind 0.0.0.0:8000
