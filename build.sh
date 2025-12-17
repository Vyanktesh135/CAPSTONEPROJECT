set -e

pip install -r requirement.txt
alembic upgrade head

# #frontend
cd frontend
npm install
npm run build