nohup gunicorn -w 5 --threads=10 -b 0.0.0.0:10086 svc-release:app >log 2>&1 &
