"""
=============================================================================
ELECTRICITY TRACKER - MAIN FLASK APPLICATION
=============================================================================
Student: Raushan Kumar
Course: Cloud Computing / AWS

This is the main backend server for the Electricity Tracker web application.
It provides REST API endpoints for:
- Uploading CSV files with electricity readings
- Viewing usage data (daily/monthly)
- Estimating electricity bills
- Sending alerts via email (SNS)

AWS Services Used:
- S3: Store uploaded CSV files in the cloud
- DynamoDB: Store electricity readings in a NoSQL database
- SNS: Send email notifications/alerts to users
- Lambda: Run serverless functions

How to run:
    python -m backend.app
    
Then visit: http://127.0.0.1:5000
=============================================================================
"""

# =============================================================================
# IMPORTS - Libraries we need for this application
# =============================================================================

# Flask - A lightweight web framework for Python
# - Flask: The main class to create our web application
# - request: Access data sent by the client (form data, JSON, files)
# - jsonify: Convert Python dictionaries to JSON responses
# - send_from_directory: Serve static files (like HTML)
from flask import Flask, request, jsonify, send_from_directory

# json - For working with JSON data (JavaScript Object Notation)
import json

# pathlib - Modern way to work with file paths in Python
from pathlib import Path

# datetime - For working with dates and times
from datetime import datetime

# os - For interacting with the operating system (environment variables, paths)
import os

# dotenv - Load environment variables from .env file
# This keeps sensitive data (like AWS keys) out of our code
from dotenv import load_dotenv

# Load environment variables from .env file
# This must be called before accessing any environment variables
load_dotenv()

# =============================================================================
# CUSTOM LIBRARY IMPORTS - Our own modules for processing electricity data
# =============================================================================

# parse_csv_string: Converts CSV text into a list of MeterReading objects
from backend.lib.smart_elec_core.io import parse_csv_string

# MeterReading: A data class representing a single electricity reading
# Contains: device_id, timestamp, kwh (kilowatt-hours)
from backend.lib.smart_elec_core.models import MeterReading

# EnergyAnalyzer: Analyzes electricity usage patterns
# Can calculate daily/monthly totals and detect usage spikes
from backend.lib.smart_elec_core.processor import EnergyAnalyzer

# BillingEstimator: Calculates estimated electricity bills
# Uses a rate (EUR/kWh) to estimate costs
from backend.lib.smart_elec_core.estimator import BillingEstimator

# =============================================================================
# AWS SERVICE INITIALIZATION
# =============================================================================
# We use environment variables to enable/disable each AWS service
# This allows the app to work even without AWS (using local storage)

# -----------------------------------------------------------------------------
# S3 SERVICE - Amazon Simple Storage Service
# -----------------------------------------------------------------------------
# S3 is used to store uploaded CSV files in the cloud
# Benefits: Durable storage, accessible from anywhere, cheap storage

# Check if S3 is enabled via environment variable
USE_S3 = os.getenv('USE_S3_STORAGE', 'false').lower() == 'true'
s3_service = None  # Will hold our S3 service instance

if USE_S3:
    try:
        # Import and initialize the S3 service
        from backend.lib.s3_service import S3Service
        s3_service = S3Service()
        # Create the S3 bucket if it doesn't exist
        s3_service.create_bucket_if_not_exists()
        print("S3 storage enabled")
    except Exception as e:
        # If S3 fails, we'll fall back to local storage
        print(f"S3 initialization failed: {e}. Using local storage.")
        USE_S3 = False

# -----------------------------------------------------------------------------
# DYNAMODB SERVICE - Amazon DynamoDB (NoSQL Database)
# -----------------------------------------------------------------------------
# DynamoDB is used to store electricity readings
# Benefits: Scalable, fast, managed by AWS (no server maintenance)

# Check if DynamoDB is enabled via environment variable
USE_DYNAMODB = os.getenv('USE_DYNAMODB', 'false').lower() == 'true'
dynamodb_service = None  # Will hold our DynamoDB service instance

if USE_DYNAMODB:
    try:
        # Import and initialize the DynamoDB service
        from backend.lib.dynamodb_service import DynamoDBService
        dynamodb_service = DynamoDBService()
        # Create the table if it doesn't exist
        dynamodb_service.create_table_if_not_exists()
        print("DynamoDB storage enabled")
    except Exception as e:
        # If DynamoDB fails, we'll fall back to local file storage
        print(f"DynamoDB initialization failed: {e}. Using local storage.")
        USE_DYNAMODB = False

# -----------------------------------------------------------------------------
# SNS SERVICE - Amazon Simple Notification Service
# -----------------------------------------------------------------------------
# SNS is used to send email alerts to users
# Benefits: Send emails without managing email servers, supports SMS too

# Check if SNS is enabled via environment variable
USE_SNS = os.getenv('USE_SNS', 'false').lower() == 'true'
sns_service = None  # Will hold our SNS service instance

if USE_SNS:
    try:
        # Import and initialize the SNS service
        from backend.lib.sns_service import SNSService
        sns_service = SNSService()
        # Create the SNS topic if it doesn't exist
        sns_service.create_topic_if_not_exists()
        print("SNS notifications enabled")
    except Exception as e:
        # If SNS fails, notifications will be disabled
        print(f"SNS initialization failed: {e}. Notifications disabled.")
        USE_SNS = False

# -----------------------------------------------------------------------------
# LAMBDA SERVICE - AWS Lambda (Serverless Functions)
# -----------------------------------------------------------------------------
# Lambda allows us to run code without managing servers
# Benefits: Pay only for what you use, auto-scaling, no server maintenance

# Check if Lambda is enabled via environment variable
USE_LAMBDA = os.getenv('USE_LAMBDA', 'false').lower() == 'true'
lambda_service = None  # Will hold our Lambda service instance

if USE_LAMBDA:
    try:
        # Import and initialize the Lambda service
        from backend.lib.lambda_service import LambdaService
        lambda_service = LambdaService()
        print("Lambda service enabled")
    except Exception as e:
        print(f"Lambda initialization failed: {e}.")
        USE_LAMBDA = False

# =============================================================================
# FLASK APPLICATION INITIALIZATION
# =============================================================================

# Create the Flask application instance
# __name__ tells Flask where to find templates and static files
app = Flask(__name__)

# =============================================================================
# LOCAL DATA STORAGE CONFIGURATION
# =============================================================================
# If AWS services are not available, we store data locally in files

# Define the directory for local data storage
DATA_DIR = Path("backend/data")
# Create the directory if it doesn't exist (parents=True creates parent folders)
DATA_DIR.mkdir(parents=True, exist_ok=True)
# Path to the local file for storing readings (JSONL = JSON Lines format)
READINGS_FILE = DATA_DIR / "readings.jsonl"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_readings_for_device(device_id: str):
    """
    Load all electricity readings for a specific device.
    
    This function first tries to load from DynamoDB (cloud database).
    If DynamoDB is not enabled, it falls back to the local JSONL file.
    
    Args:
        device_id (str): The ID of the device (e.g., "device-001")
    
    Returns:
        list: A list of MeterReading objects for the device
    
    Example:
        readings = load_readings_for_device("device-001")
        for r in readings:
            print(f"{r.timestamp}: {r.kwh} kWh")
    """
    
    # OPTION 1: Load from DynamoDB (if enabled)
    if USE_DYNAMODB and dynamodb_service:
        # Query DynamoDB for all readings with this device_id
        readings_data = dynamodb_service.get_readings_for_device(device_id)
        readings = []
        
        # Convert each dictionary to a MeterReading object
        for r in readings_data:
            try:
                # Parse the ISO timestamp string to a datetime object
                # Remove any suffix like "_0" we added for uniqueness
                ts_str = r["timestamp"].split("_")[0].replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_str)
            except:
                # If parsing fails, use current time
                ts = datetime.now()
            readings.append(MeterReading(
                device_id=r["device_id"],
                timestamp=ts,
                kwh=float(r["kwh"])
            ))
        return readings
    
    # OPTION 2: Load from local file (fallback)
    if not READINGS_FILE.exists():
        return []  # No data yet
    
    # Use a dictionary to deduplicate readings by (device_id, timestamp)
    # If the same reading is uploaded twice, we keep the latest one
    seen = {}
    
    # Read the JSONL file line by line
    with READINGS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            # Parse each line as JSON
            obj = json.loads(line)
            
            # Skip readings for other devices
            if obj.get("device_id") != device_id:
                continue
            
            # Parse the timestamp
            ts = datetime.fromisoformat(obj["timestamp"])
            
            # Create a unique key for deduplication
            key = (obj["device_id"], obj["timestamp"])
            
            # Store the reading (overwrites if duplicate)
            seen[key] = MeterReading(
                device_id=obj["device_id"],
                timestamp=ts,
                kwh=float(obj["kwh"])
            )
    
    # Return all unique readings as a list
    return list(seen.values())

# =============================================================================
# API ROUTES - BASIC ENDPOINTS
# =============================================================================

@app.route("/")
def home():
    """
    Serve the frontend HTML page.
    
    When a user visits http://127.0.0.1:5000/, this function
    sends them the index.html file from the frontend folder.
    
    Returns:
        The index.html file
    """
    frontend_path = os.path.join(os.getcwd(), "frontend")
    return send_from_directory(frontend_path, "index.html")


@app.route("/upload", methods=["POST"])
def upload():
    """
    Handle CSV file uploads.
    
    This endpoint:
    1. Receives a CSV file from the frontend
    2. Parses the CSV to extract electricity readings
    3. Stores the readings in DynamoDB (or local file as fallback)
    4. Optionally stores the CSV file in S3 for backup
    
    Expected CSV format:
        device_id,timestamp,kwh
        device-001,2025-11-01T00:00:00Z,0.34
        device-001,2025-11-01T01:00:00Z,0.29
    
    Returns:
        JSON with upload_id and processed_count
    
    HTTP Status Codes:
        202: Accepted - Upload successful
        400: Bad Request - No file provided
    """
    # Check if a file was included in the request
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    # Get the uploaded file
    file = request.files["file"]
    
    # Read the file content as bytes, then decode to string
    content_bytes = file.read()
    content = content_bytes.decode("utf-8")

    # Parse the CSV content into MeterReading objects
    readings = parse_csv_string(content)

    # Store readings in database
    dynamodb_count = 0
    
    if USE_DYNAMODB and dynamodb_service:
        # OPTION 1: Store in DynamoDB (cloud database)
        # Convert MeterReading objects to dictionaries for DynamoDB
        # Use current timestamp (unique for each reading)
        readings_data = []
        for i, r in enumerate(readings):
            # Create unique timestamp for each reading
            unique_time = datetime.now().isoformat() + f"_{i}"
            readings_data.append({
                "device_id": r.device_id,
                "timestamp": unique_time,  # Unique timestamp for DynamoDB key
                "kwh": r.kwh
            })
        # Batch write to DynamoDB (efficient for multiple items)
        dynamodb_count = dynamodb_service.put_readings_batch(readings_data)
    else:
        # OPTION 2: Store in local JSONL file (fallback)
        # Open file in append mode to add new readings
        with READINGS_FILE.open("a", encoding="utf-8") as f:
            for r in readings:
                # Write each reading as a JSON line
                f.write(json.dumps({
                    "device_id": r.device_id,
                    "timestamp": r.timestamp.isoformat(),
                    "kwh": r.kwh
                }) + "\n")

    # Optionally backup the CSV file to S3
    s3_key = None
    if USE_S3 and s3_service:
        # Upload the original CSV to S3 with a unique key
        s3_key = s3_service.upload_file(content_bytes, file.filename)

    # Build the response
    response = {
        "upload_id": file.filename,
        "processed_count": len(readings)
    }
    
    # Add S3 key if file was uploaded to S3
    if s3_key:
        response["s3_key"] = s3_key
    
    # Add DynamoDB count if using DynamoDB
    if USE_DYNAMODB:
        response["dynamodb_count"] = dynamodb_count
    
    # ==========================================================
    # HIGH USAGE ALERT - Send SNS notification if kWh > 4400
    # ==========================================================
    HIGH_USAGE_THRESHOLD = 4400  # kWh threshold for alert
    
    # Check if any reading exceeds the threshold
    for r in readings:
        if r.kwh > HIGH_USAGE_THRESHOLD:
            # Send SNS alert if SNS is enabled
            if USE_SNS and sns_service:
                try:
                    alert_message = f"""
⚠️ HIGH ELECTRICITY USAGE ALERT ⚠️

Device ID: {r.device_id}
Usage: {r.kwh} kWh
Threshold: {HIGH_USAGE_THRESHOLD} kWh

Your electricity consumption has exceeded the safe limit!
Please check your appliances and reduce usage if possible.

- Smart Electricity Tracker
"""
                    sns_service.send_alert(
                        subject="⚠️ HIGH USAGE ALERT - Electricity Tracker",
                        message=alert_message
                    )
                    response["alert_sent"] = True
                    response["alert_reason"] = f"Usage {r.kwh} kWh exceeds threshold {HIGH_USAGE_THRESHOLD} kWh"
                    print(f"SNS Alert sent for high usage: {r.kwh} kWh")
                except Exception as e:
                    print(f"Failed to send SNS alert: {e}")
                    response["alert_error"] = str(e)
            break  # Only send one alert per upload
    
    # Return 202 Accepted (processing complete)
    return jsonify(response), 202


@app.route("/usage", methods=["GET"])
def usage():
    """
    Get electricity usage data for a device.
    
    Query Parameters:
        device_id (required): The device ID to get usage for
        period (optional): 'day' or 'month' (default: 'day')
    
    Returns:
        JSON with device_id, period, and usage data
    
    Example Request:
        GET /usage?device_id=device-001&period=day
    
    Example Response:
        {
            "device_id": "device-001",
            "period": "day",
            "data": [
                {"period": "2025-11-01", "total_kwh": 0.94}
            ]
        }
    """
    # Get query parameters from the URL
    device_id = request.args.get("device_id")
    period = request.args.get("period", "day")  # Default to 'day'
    
    # Validate required parameter
    if not device_id:
        return jsonify({"error": "device_id required"}), 400

    # Load readings for this device
    readings = load_readings_for_device(device_id)
    
    # Create an analyzer to process the readings
    analyzer = EnergyAnalyzer(readings)
    
    # Aggregate usage by day or month
    if period == "day":
        data = analyzer.daily_usage()
    else:
        data = analyzer.monthly_usage()
    
    # Convert to list of objects for JSON response
    data_list = [{"period": k, "total_kwh": v} for k, v in sorted(data.items())]
    
    return jsonify({
        "device_id": device_id,
        "period": period,
        "data": data_list
    })


@app.route("/readings", methods=["GET"])
def get_readings():
    """
    Get raw electricity readings for a device with created_at date.
    
    This endpoint returns individual readings instead of aggregated data,
    showing when each reading was created in the system.
    
    Query Parameters:
        device_id (required): The device ID to get readings for
    
    Returns:
        JSON with device_id and list of readings with created_at
    
    Example Request:
        GET /readings?device_id=meter-home-1
    
    Example Response:
        {
            "device_id": "meter-home-1",
            "readings": [
                {"kwh": 1.5, "created_at": "2025-11-30T16:05:49"}
            ]
        }
    """
    # Get the device_id from query parameters
    device_id = request.args.get("device_id")
    
    # Validate required parameter
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    readings_data = []
    
    # Check DynamoDB first (if enabled)
    if USE_DYNAMODB and dynamodb_service:
        try:
            # Query DynamoDB for readings
            items = dynamodb_service.get_readings_for_device(device_id)
            for item in items:
                readings_data.append({
                    "kwh": float(item.get("kwh", 0)),
                    "created_at": item.get("created_at", "N/A")
                })
        except Exception as e:
            print(f"DynamoDB query failed: {e}")
    
    # Fallback to local file if no DynamoDB data
    if not readings_data and READINGS_FILE.exists():
        with READINGS_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                if obj.get("device_id") == device_id:
                    readings_data.append({
                        "kwh": float(obj.get("kwh", 0)),
                        "created_at": obj.get("timestamp", "N/A")
                    })
    
    return jsonify({
        "device_id": device_id,
        "readings": readings_data
    })


@app.route("/anomalies", methods=["GET"])
def anomalies():
    """
    Detect usage spikes/anomalies for a device.
    
    A spike is detected when usage increases by more than threshold_pct
    compared to the previous day.
    
    Query Parameters:
        device_id (required): The device ID to analyze
        threshold_pct (optional): Percentage threshold (default: 50.0)
    
    Returns:
        JSON with detected spikes (date, previous kWh, current kWh)
    
    Example:
        If yesterday was 5 kWh and today is 10 kWh, that's a 100% increase.
        With threshold_pct=50, this would be flagged as a spike.
    """
    # Get query parameters
    device_id = request.args.get("device_id")
    
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    # Parse threshold (with error handling)
    try:
        threshold = float(request.args.get("threshold_pct", 50.0))
    except ValueError:
        return jsonify({"error": "threshold_pct must be a number"}), 400

    # Load and analyze readings
    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    
    # Detect spikes above the threshold
    spikes = analyzer.detect_spikes(threshold_pct=threshold)
    
    # Format spikes for JSON response
    formatted = [
        {"date": d, "prev_kwh": p, "curr_kwh": c} 
        for d, p, c in spikes
    ]

    return jsonify({
        "device_id": device_id,
        "threshold_pct": threshold,
        "spikes": formatted
    })


@app.route("/estimate", methods=["GET"])
def estimate():
    """
    Estimate electricity bill for a device.
    
    Calculates the total cost based on usage and rate per kWh.
    
    Query Parameters:
        device_id (required): The device ID
        rate (optional): Rate per kWh in EUR (default: 0.20)
        period (optional): 'day' or 'month' (default: 'day')
    
    Returns:
        JSON with estimated cost and rate information
    
    Example:
        GET /estimate?device_id=device-001&rate=0.25&period=day
        
        Response:
        {
            "device_id": "device-001",
            "period": "day",
            "estimated_cost": 2.35,
            "rate_per_kwh": 0.25,
            "currency": "EUR"
        }
    """
    # Get and validate parameters
    device_id = request.args.get("device_id")
    
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    # Parse rate with error handling
    try:
        rate = float(request.args.get("rate", 0.20))
    except ValueError:
        return jsonify({"error": "rate must be a number"}), 400

    # Validate period
    period = request.args.get("period", "day").lower()
    if period not in ("day", "month"):
        return jsonify({"error": "period must be 'day' or 'month'"}), 400

    # Load readings and calculate usage
    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    usage = analyzer.daily_usage() if period == "day" else analyzer.monthly_usage()
    
    # Calculate estimated cost
    estimator = BillingEstimator(rate)
    cost = estimator.estimate_cost(usage)

    return jsonify({
        "device_id": device_id,
        "period": period,
        "estimated_cost": cost,
        "rate_per_kwh": rate,
        "currency": "EUR"
    })


# =============================================================================
# API ROUTES - S3 ENDPOINTS
# =============================================================================

@app.route("/s3/files", methods=["GET"])
def list_s3_files():
    """
    List all files stored in the S3 bucket.
    
    Returns a list of uploaded CSV files with their metadata.
    
    Returns:
        JSON with list of files and bucket name
    """
    if not USE_S3 or not s3_service:
        return jsonify({"error": "S3 storage not enabled"}), 400
    
    files = s3_service.list_files()
    return jsonify({"files": files, "bucket": s3_service.bucket_name})


@app.route("/s3/status", methods=["GET"])
def s3_status():
    """
    Check S3 service status.
    
    Returns whether S3 is enabled and the bucket name.
    Useful for debugging and health checks.
    """
    return jsonify({
        "s3_enabled": USE_S3,
        "bucket_name": s3_service.bucket_name if s3_service else None
    })


# =============================================================================
# API ROUTES - DYNAMODB ENDPOINTS
# =============================================================================

@app.route("/dynamodb/status", methods=["GET"])
def dynamodb_status():
    """
    Check DynamoDB service status.
    
    Returns whether DynamoDB is enabled and the table name.
    """
    return jsonify({
        "dynamodb_enabled": USE_DYNAMODB,
        "table_name": dynamodb_service.table_name if dynamodb_service else None
    })


@app.route("/dynamodb/devices", methods=["GET"])
def list_devices():
    """
    List all devices that have data in DynamoDB.
    
    Scans the table and returns unique device IDs.
    """
    if not USE_DYNAMODB or not dynamodb_service:
        return jsonify({"error": "DynamoDB not enabled"}), 400
    
    devices = dynamodb_service.get_all_devices()
    return jsonify({"devices": devices})


# =============================================================================
# API ROUTES - SNS ENDPOINTS (Email Notifications)
# =============================================================================

@app.route("/sns/status", methods=["GET"])
def sns_status():
    """
    Check SNS service status.
    
    Returns whether SNS is enabled and the topic ARN.
    """
    return jsonify({
        "sns_enabled": USE_SNS,
        "topic_arn": sns_service.topic_arn if sns_service else None
    })


@app.route("/sns/subscribe", methods=["POST"])
def sns_subscribe():
    """
    Subscribe an email address to receive alerts.
    
    The user will receive a confirmation email from AWS.
    They must click the link to confirm their subscription.
    
    Request Body (JSON):
        {"email": "user@example.com"}
    
    Returns:
        Confirmation message with subscription ARN
    """
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    # Get email from request body
    data = request.get_json()
    if not data or not data.get("email"):
        return jsonify({"error": "email required"}), 400
    
    email = data["email"]
    
    # Subscribe the email to the SNS topic
    subscription_arn = sns_service.subscribe_email(email)
    
    if subscription_arn:
        return jsonify({
            "message": f"Subscription pending. Check {email} for confirmation link.",
            "subscription_arn": subscription_arn
        })
    else:
        return jsonify({"error": "Failed to subscribe"}), 500


@app.route("/sns/subscriptions", methods=["GET"])
def sns_subscriptions():
    """
    List all email subscriptions.
    
    Shows all subscribers to the alert topic and their status.
    """
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    subscriptions = sns_service.list_subscriptions()
    return jsonify({"subscriptions": subscriptions})


@app.route("/sns/test", methods=["POST"])
def sns_test_alert():
    """
    Send a test alert to all subscribers.
    
    Useful for verifying that email notifications are working.
    """
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


@app.route("/sns/alert/usage", methods=["POST"])
def sns_usage_alert():
    """
    Check usage and send alerts if threshold is exceeded.
    
    Analyzes daily usage and sends an email alert for each day
    where usage exceeds the specified threshold.
    
    Request Body (JSON):
        {
            "device_id": "device-001",
            "threshold_kwh": 10.0
        }
    """
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    data = request.get_json()
    device_id = data.get("device_id")
    threshold_kwh = float(data.get("threshold_kwh", 10.0))
    
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    # Get current usage for the device
    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    daily = analyzer.daily_usage()
    
    # Check each day and send alerts for high usage
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


@app.route("/sns/alert/spikes", methods=["POST"])
def sns_spike_alert():
    """
    Detect spikes and send alerts.
    
    Analyzes usage patterns and sends an email alert for each
    detected spike (abnormal increase in usage).
    
    Request Body (JSON):
        {
            "device_id": "device-001",
            "threshold_pct": 50.0
        }
    """
    if not USE_SNS or not sns_service:
        return jsonify({"error": "SNS not enabled"}), 400
    
    data = request.get_json() or {}
    device_id = data.get("device_id")
    threshold_pct = float(data.get("threshold_pct", 50.0))
    
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    
    # Detect spikes in usage
    readings = load_readings_for_device(device_id)
    analyzer = EnergyAnalyzer(readings)
    spikes = analyzer.detect_spikes(threshold_pct=threshold_pct)
    
    # Send an alert for each spike
    alerts_sent = 0
    for date, prev_kwh, curr_kwh in spikes:
        # Calculate percentage change
        change_pct = (curr_kwh - prev_kwh) / prev_kwh * 100 if prev_kwh > 0 else 0
        sns_service.send_spike_alert(device_id, date, prev_kwh, curr_kwh, change_pct)
        alerts_sent += 1
    
    return jsonify({
        "message": f"Checked spikes for {device_id}",
        "spikes_found": len(spikes),
        "alerts_sent": alerts_sent
    })


# =============================================================================
# API ROUTES - LAMBDA ENDPOINTS (Serverless Functions)
# =============================================================================

@app.route("/lambda/status", methods=["GET"])
def lambda_status():
    """
    Check Lambda service status.
    
    Returns whether Lambda is enabled and available.
    """
    return jsonify({
        "lambda_enabled": USE_LAMBDA,
        "service_available": lambda_service is not None
    })


@app.route("/lambda/functions", methods=["GET"])
def list_lambda_functions():
    """
    List all Lambda functions in the AWS account.
    
    Returns the names and count of available Lambda functions.
    """
    if not USE_LAMBDA or not lambda_service:
        return jsonify({"error": "Lambda not enabled"}), 400
    
    functions = lambda_service.list_functions()
    function_names = [f['FunctionName'] for f in functions]
    return jsonify({"functions": function_names, "count": len(functions)})


@app.route("/lambda/invoke", methods=["POST"])
def invoke_lambda():
    """
    Invoke a Lambda function.
    
    Calls a Lambda function with the specified payload and
    returns the result.
    
    Request Body (JSON):
        {
            "function_name": "electricity-get-usage",
            "payload": {"device_id": "device-001"}
        }
    """
    if not USE_LAMBDA or not lambda_service:
        return jsonify({"error": "Lambda not enabled"}), 400
    
    data = request.get_json()
    if not data or not data.get("function_name"):
        return jsonify({"error": "function_name required"}), 400
    
    function_name = data["function_name"]
    payload = data.get("payload", {})
    
    # Invoke the Lambda function and get the result
    result = lambda_service.invoke_function(function_name, payload)
    
    if result:
        return jsonify({"result": result})
    else:
        return jsonify({"error": "Failed to invoke function"}), 500


# =============================================================================
# RUN THE SERVER
# =============================================================================

if __name__ == "__main__":
    """
    Start the Flask development server.
    
    This block only runs when executing the file directly:
        python -m backend.app
    
    It does NOT run when importing this module.
    
    debug=True enables:
    - Auto-reload when code changes
    - Detailed error messages
    - Interactive debugger
    
    WARNING: Never use debug=True in production!
    """
    app.run(debug=True)
