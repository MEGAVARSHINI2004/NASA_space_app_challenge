from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from data_processor import NASADataProcessor
import tempfile

app = Flask(__name__)

# Initialize the data processor
data_processor = NASADataProcessor()

@app.route('/')
def index():
    """Serve the main dashboard page"""
    return render_template('index.html')

@app.route('/api/weather-probability', methods=['POST'])
def get_weather_probability():
    """API endpoint to get weather probabilities for a location and date"""
    try:
        data = request.get_json()
        
        # Validate required parameters
        if not data:
            return jsonify({'success': False, 'error': 'No JSON data provided'}), 400
        
        lat = data.get('latitude')
        lng = data.get('longitude')
        target_date = data.get('date')
        
        if lat is None or lng is None or not target_date:
            return jsonify({
                'success': False, 
                'error': 'Missing required parameters: latitude, longitude, and date are required'
            }), 400
        
        # Convert to appropriate types
        try:
            lat = float(lat)
            lng = float(lng)
        except (ValueError, TypeError):
            return jsonify({
                'success': False, 
                'error': 'Invalid latitude or longitude format'
            }), 400
        
        # Validate date
        try:
            datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'success': False, 
                'error': 'Invalid date format. Use YYYY-MM-DD'
            }), 400
        
        # Calculate probabilities
        probabilities = data_processor.calculate_probabilities(lat, lng, target_date)
        
        return jsonify({
            'success': True,
            'probabilities': probabilities,
            'location': f"{lat:.4f}, {lng:.4f}",
            'date': target_date,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        app.logger.error(f"Error in weather-probability endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/historical-trends', methods=['POST'])
def get_historical_trends():
    """API endpoint to get historical weather trends for a location"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No JSON data provided'}), 400
        
        lat = data.get('latitude')
        lng = data.get('longitude')
        
        if lat is None or lng is None:
            return jsonify({
                'success': False, 
                'error': 'Missing required parameters: latitude and longitude are required'
            }), 400
        
        # Convert to appropriate types
        try:
            lat = float(lat)
            lng = float(lng)
        except (ValueError, TypeError):
            return jsonify({
                'success': False, 
                'error': 'Invalid latitude or longitude format'
            }), 400
        
        # Get historical trends
        trends = data_processor.get_historical_trends(lat, lng)
        
        return jsonify({
            'success': True,
            'trends': trends,
            'location': f"{lat:.4f}, {lng:.4f}",
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        app.logger.error(f"Error in historical-trends endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/download-data', methods=['POST'])
def download_data():
    """API endpoint to download weather probability data as CSV"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No JSON data provided'}), 400
        
        lat = data.get('latitude')
        lng = data.get('longitude')
        target_date = data.get('date')
        
        if lat is None or lng is None or not target_date:
            return jsonify({
                'success': False, 
                'error': 'Missing required parameters: latitude, longitude, and date are required'
            }), 400
        
        # Convert to appropriate types
        try:
            lat = float(lat)
            lng = float(lng)
        except (ValueError, TypeError):
            return jsonify({
                'success': False, 
                'error': 'Invalid latitude or longitude format'
            }), 400
        
        # Validate date
        try:
            datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'success': False, 
                'error': 'Invalid date format. Use YYYY-MM-DD'
            }), 400
        
        # Generate CSV data
        csv_data = data_processor.generate_csv_data(lat, lng, target_date)
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_data.to_csv(f.name, index=False)
            temp_filename = f.name
        
        # Send file
        download_filename = f"weather_probability_{lat:.4f}_{lng:.4f}_{target_date}.csv"
        response = send_file(
            temp_filename,
            as_attachment=True,
            download_name=download_filename,
            mimetype='text/csv'
        )
        
        # Clean up temporary file after send
        @response.call_on_close
        def cleanup_temp_file():
            try:
                os.unlink(temp_filename)
            except OSError:
                pass
        
        return response
        
    except Exception as e:
        app.logger.error(f"Error in download-data endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'data_loaded': data_processor.data_loaded
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({'success': False, 'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'success': False, 'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("Starting Weather Probability Dashboard...")
    print("Access the application at: http://localhost:5000")
    print("API Health check: http://localhost:5000/api/health")
    app.run(debug=True, host='0.0.0.0', port=5000)