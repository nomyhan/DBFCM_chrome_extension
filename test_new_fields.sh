#!/bin/bash
# Test the updated backend with new fields

echo "Starting backend server on port 8888..."
python3 backend_server.py 8888 > /tmp/backend_test.log 2>&1 &
BACKEND_PID=$!
echo "Backend PID: $BACKEND_PID"

echo "Waiting for server to start..."
sleep 3

echo ""
echo "Testing API endpoint..."
curl -s http://localhost:8888/api/waitlist | python3 -m json.tool | head -80

echo ""
echo "Stopping server..."
kill $BACKEND_PID 2>/dev/null
wait $BACKEND_PID 2>/dev/null

echo ""
echo "Test complete!"
