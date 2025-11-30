"""
Elastic Beanstalk Entry Point
"""
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Now import the Flask app
from backend.app import app as application

# For local testing
if __name__ == "__main__":
    application.run(debug=True)
