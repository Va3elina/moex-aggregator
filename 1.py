
content = '''services:
  - type: web
    name: moex-aggregator
    runtime: python
    buildCommand: |
      pip install -r requirements.txt
      cd frontend && npm install && npm run build
    startCommand: uvicorn api.main:app --host 0.0.0.0 --port $PORT
    envVars:
      - key: PYTHON_VERSION
        value: \"3.11\"
      - key: NODE_VERSION
        value: \"20\"
'''
with open('render.yaml', 'w', encoding='utf-8') as f:
    f.write(content)
print('render.yaml created!')
