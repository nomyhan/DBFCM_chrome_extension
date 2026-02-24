#!/bin/bash
# Test the backend server

echo "Starting backend server..."
python3 backend_server.py 8000 &
BACKEND_PID=$!

echo "Waiting for server to start..."
sleep 3

echo ""
echo "Testing API endpoint..."
curl -s http://localhost:8000/api/waitlist | python3 -m json.tool | head -50

echo ""
echo "Stopping server..."
kill $BACKEND_PID 2>/dev/null
wait $BACKEND_PID 2>/dev/null

echo "Test complete!"
