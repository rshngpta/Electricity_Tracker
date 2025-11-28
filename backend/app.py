# backend/app.py

# -------------------------
# IMPORTS
# -------------------------
from flask import Flask, request, jsonify, send_from_directory
import json
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Our library imports (ensure these exist in your project)
from backend.lib.smart_elec_core.io import parse_csv_string
from backend.lib.smart_elec_core.models import MeterReading
from backend.lib.smart_elec_core.processor import EnergyAnalyzer
from backend.lib.smart_elec_core.estimator import BillingEstimator

# S3 Service (optional - only if USE_S3_STORAGE is enabled)
USE_S3 = os.getenv('USE_S3_STORAGE', 'false').lower() == 'true'
s3_service = None

if USE_S3:
    try:
        from backend.lib.s3_service import S3Service
        s3_service = S3Service()
        s3_service.create_bucket_if_not_exists()
        print("S3 storage enabled")
    except Exception as e:
        print(f"S3 initialization failed: {e}. Using local storage.")
        USE_S3 = False

# DynamoDB Service (optional - only if USE_DYNAMODB is enabled)
USE_DYNAMODB = os.getenv('USE_DYNAMODB', 'false').lower() == 'true'
dynamodb_service = None

if USE_DYNAMODB:
    try:
        from backend.lib.dynamodb_service import DynamoDBService
        dynamodb_service = DynamoDBService()
        dynamodb_service.create_table_if_not_exists()
        print("DynamoDB storage enabled")
    except Exception as e:
        print(f"DynamoDB initialization failed: {e}. Using local storage.")
        USE_DYNAMODB = False

# SNS Service (optional - only if USE_SNS is enabled)
USE_SNS = os.getenv('USE_SNS', 'false').lower() == 'true'
sns_service = None

if USE_SNS:
    try:
        from backend.lib.sns_service import SNSService
        sns_service = SNSService()
        sns_service.create_topic_if_not_exists()
        print("SNS notifications enabled")
    except Exception as e:
        print(f"SNS initialization failed: {e}. Notifications disabled.")
        USE_SNS = False

# Lambda Service (optional - only if USE_LAMBDA is enabled)
USE_LAMBDA = os.getenv('USE_LAMBDA', 'false').lower() == 'true'
lambda_service = None

if USE_LAMBDA:
    try:
        from backend.lib.lambda_service import LambdaService
        lambda_service = LambdaService()
        print("Lambda service enabled")
    except Exception as e:
        print(f"Lambda initialization failed: {e}.")
        USE_LAMBDA = False

# -------------------------
# FLASK INITIALIZATION
# -------------------------
app = Flask(__name__)

# -------------------------
# DATA PATHS
# -------------------------
DATA_DIR = Path("backend/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
READINGS_FILE = DATA_DIR / "readings.jsonl"

# -------------------------
# HELPER FUNCTION
# -------------------------
def load_readings_for_device(device_id: str):
    # If DynamoDB is enabled, read from there
    if USE_DYNAMODB and dynamodb_service:
        readings_data = dynamodb_service.get_readings_for_device(device_id)
        readings = []
        for r in readings_data:
            ts = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
            readings.append(MeterReading(
                device_id=r["device_id"],
                timestamp=ts,
                kwh=float(r["kwh"])
            ))
        return readings
    
    # Fallback to local file
    if not READINGS_FILE.exists():
        return []
    # Use dict to deduplicate by (device_id, timestamp) - last write wins
    seen = {}
    with READINGS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if obj.get("device_id") != device_id:
                continue
            ts = datetime.fromisoformat(obj["timestamp"])
            key = (obj["device_id"], obj["timestamp"])
            seen[key] = MeterReading(
                device_id=obj["device_id"],
                timestamp=ts,
                kwh=float(obj["kwh"])
            )
    return list(seen.values())

# -------------------------
# ROUTES
# -------------------------

# Serve frontend
@app.route("/")
def home():
    frontend_path = os.path.join(os.getcwd(), "frontend")
    return send_from_directory(frontend_path, "index.html")

# CSV Upload
@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    content_bytes = file.read()
    content = content_bytes.decode("utf-8")

    readings = parse_csv_string(content)

    # Store in DynamoDB if enabled
    dynamodb_count = 0
    if USE_DYNAMODB and dynamodb_service:
        readings_data = [
            {
                "device_id": r.device_id,
                "timestamp": r.timestamp.isoformat(),
                "kwh": r.kwh
            }
            for r in readings
        ]
        dynamodb_count = dynamodb_service.put_readings_batch(readings_data)
    else:
        # Fallback: Store locally
        with READINGS_FILE.open("a", encoding="utf-8") as f:
            for r in readings:
                f.write(json.dumps({
                    "device_id": r.device_id,
                    "timestamp": r.timestamp.isoformat(),
                    "kwh": r.kwh
                }) + "\n")

    # Also store CSV in S3 if enabled
    s3_key = None
    if USE_S3 and s3_service:
        s3_key = s3_service.upload_file(content_bytes, file.filename)

    response = {"upload_id": file.filename, "processed_count": len(readings)}
    if s3_key:
        response["s3_key"] = s3_key
    if USE_DYNAMODB:
        response["dynamodb_count"] = dynamodb_count
    
    return jsonify(response), 202

# Usage endpoint
@app.route("/usage", methods=["GET"])
def usage():
    device_id = request.args.get("device_id")
    period = request.args.get("period", "day")
    if not device_id:
        return jsonify({"error": "device_id required"}), 400

    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    data = analyzer.daily_usage() if period == "day" else analyzer.monthly_usage()
    data_list = [{"period": k, "total_kwh": v} for k, v in sorted(data.items())]
    return jsonify({"device_id": device_id, "period": period, "data": data_list})

# Anomalies endpoint
@app.route("/anomalies", methods=["GET"])
def anomalies():
    device_id = request.args.get("device_id")
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    try:
        threshold = float(request.args.get("threshold_pct", 50.0))
    except ValueError:
        return jsonify({"error": "threshold_pct must be a number"}), 400

    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    spikes = analyzer.detect_spikes(threshold_pct=threshold)
    formatted = [{"date": d, "prev_kwh": p, "curr_kwh": c} for d, p, c in spikes]

    return jsonify({"device_id": device_id, "threshold_pct": threshold, "spikes": formatted})

# Estimate endpoint
@app.route("/estimate", methods=["GET"])
def estimate():
    device_id = request.args.get("device_id")
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    try:
        rate = float(request.args.get("rate", 0.20))
    except ValueError:
        return jsonify({"error": "rate must be a number"}), 400

    period = request.args.get("period", "day").lower()
    if period not in ("day", "month"):
        return jsonify({"error": "period must be 'day' or 'month'"}), 400

    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    usage = analyzer.daily_usage() if period == "day" else analyzer.monthly_usage()
    estimator = BillingEstimator(rate)
    cost = estimator.estimate_cost(usage)

    return jsonify({
        "device_id": device_id,
        "period": period,
        "estimated_cost": cost,
        "rate_per_kwh": rate,
        "currency": "EUR"
    })

# S3 Files endpoint - list uploaded files in S3
@app.route("/s3/files", methods=["GET"])
def list_s3_files():
    if not USE_S3 or not s3_service:
        return jsonify({"error": "S3 storage not enabled"}), 400
    
    files = s3_service.list_files()
    return jsonify({"files": files, "bucket": s3_service.bucket_name})

# S3 Status endpoint - check S3 configuration
@app.route("/s3/status", methods=["GET"])
def s3_status():
    return jsonify({
        "s3_enabled": USE_S3,
        "bucket_name": s3_service.bucket_name if s3_service else None
    })

# DynamoDB Status endpoint - check DynamoDB configuration
@app.route("/dynamodb/status", methods=["GET"])
def dynamodb_status():
    return jsonify({
        "dynamodb_enabled": USE_DYNAMODB,
        "table_name": dynamodb_service.table_name if dynamodb_service else None
    })

# DynamoDB Devices endpoint - list all devices
@app.route("/dynamodb/devices", methods=["GET"])
def list_devices():
    if not USE_DYNAMODB or not dynamodb_service:
        return jsonify({"error": "DynamoDB not enabled"}), 400
    
    devices = dynamodb_service.get_all_devices()
    return jsonify({"devices": devices})

# -------------------------
# SNS ENDPOINTS
# -------------------------

# SNS Status endpoint
@app.route("/sns/status", methods=["GET"])
def sns_status():
    return jsonify({
        "sns_enabled": USE_SNS,
        "topic_arn": sns_service.topic_arn if sns_service else None
    })

# Subscribe email to alerts
@app.route("/sns/subscribe", methods=["POST"])
def sns_subscribe():
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    data = request.get_json()
    if not data or not data.get("email"):
        return jsonify({"error": "email required"}), 400
    
    email = data["email"]
    subscription_arn = sns_service.subscribe_email(email)
    
    if subscription_arn:
        return jsonify({
            "message": f"Subscription pending. Check {email} for confirmation link.",
            "subscription_arn": subscription_arn
        })
    else:
        return jsonify({"error": "Failed to subscribe"}), 500

# List subscriptions
@app.route("/sns/subscriptions", methods=["GET"])
def sns_subscriptions():
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    subscriptions = sns_service.list_subscriptions()
    return jsonify({"subscriptions": subscriptions})

# Send test alert
@app.route("/sns/test", methods=["POST"])
def sns_test_alert():
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    success = sns_service.send_alert(
        subject="🔌 Test Alert - Electricity Tracker",
        message="This is a test notification from your Electricity Tracker app.\n\nIf you received this, SNS is working correctly!"
    )
    
    if success:
        return jsonify({"message": "Test alert sent successfully"})
    else:
        return jsonify({"error": "Failed to send alert"}), 500

# Send usage alert
@app.route("/sns/alert/usage", methods=["POST"])
def sns_usage_alert():
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    data = request.get_json()
    device_id = data.get("device_id")
    threshold_kwh = float(data.get("threshold_kwh", 10.0))
    
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    # Get current usage
    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    daily = analyzer.daily_usage()
    
    # Check if any day exceeds threshold
    alerts_sent = 0
    for date, kwh in daily.items():
        if kwh > threshold_kwh:
            sns_service.send_usage_alert(device_id, kwh, threshold_kwh)
            alerts_sent += 1
    
    return jsonify({
        "message": f"Checked usage for {device_id}",
        "alerts_sent": alerts_sent,
        "threshold_kwh": threshold_kwh
    })

# Check and alert on spikes
@app.route("/sns/alert/spikes", methods=["POST"])
def sns_spike_alert():
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    data = request.get_json() or {}
    device_id = data.get("device_id")
    threshold_pct = float(data.get("threshold_pct", 50.0))
    
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    # Detect spikes
    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    spikes = analyzer.detect_spikes(threshold_pct=threshold_pct)
    
    # Send alerts for each spike
    alerts_sent = 0
    for date, prev_kwh, curr_kwh in spikes:
        change_pct = (curr_kwh - prev_kwh) / prev_kwh * 100 if prev_kwh > 0 else 0
        sns_service.send_spike_alert(device_id, date, prev_kwh, curr_kwh, change_pct)
        alerts_sent += 1
    
    return jsonify({
        "message": f"Checked spikes for {device_id}",
        "spikes_found": len(spikes),
        "alerts_sent": alerts_sent
    })

# -------------------------
# LAMBDA ENDPOINTS
# -------------------------

# Lambda Status endpoint
@app.route("/lambda/status", methods=["GET"])
def lambda_status():
    return jsonify({
        "lambda_enabled": USE_LAMBDA,
        "service_available": lambda_service is not None
    })

# List Lambda functions
@app.route("/lambda/functions", methods=["GET"])
def list_lambda_functions():
    if not USE_LAMBDA or not lambda_service:
        return jsonify({"error": "Lambda not enabled"}), 400
    
    functions = lambda_service.list_functions()
    function_names = [f['FunctionName'] for f in functions]
    return jsonify({"functions": function_names, "count": len(functions)})

# Invoke Lambda function
@app.route("/lambda/invoke", methods=["POST"])
def invoke_lambda():
    if not USE_LAMBDA or not lambda_service:
        return jsonify({"error": "Lambda not enabled"}), 400
    
    data = request.get_json()
    if not data or not data.get("function_name"):
        return jsonify({"error": "function_name required"}), 400
    
    function_name = data["function_name"]
    payload = data.get("payload", {})
    
    result = lambda_service.invoke_function(function_name, payload)
    
    if result:
        return jsonify({"result": result})
    else:
        return jsonify({"error": "Failed to invoke function"}), 500

# -------------------------
# RUN SERVER
# -------------------------
if __name__ == "__main__":
    app.run(debug=True)
