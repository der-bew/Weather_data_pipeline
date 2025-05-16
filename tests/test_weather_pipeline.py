#!/usr/bin/env python3
"""
Tests for the Weather Data Processing Pipeline.
"""
# Add parent directory to path to import the pipeline
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from weather_pipeline import WeatherDataPipeline


class TestWeatherDataPipeline(unittest.TestCase):
    """Test cases for the WeatherDataPipeline class."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test data and output directory."""
        cls.test_dir = Path(__file__).parent / 'test_data'
        cls.test_dir.mkdir(exist_ok=True)
        
        # Create test data with valid and invalid dates
        cls.test_data = [
            ['2023-01-01', 'TestCity', 25.0, 60.0, 10.0, 'sunny'],
            ['2023-01-02', 'TestCity', None, 65.0, 12.0, 'cloudy'],
            ['2023-01-03', 'TestCity', 22.0, None, 8.0, 'rainy'],
            ['2023-01-04', 'TestCity', 20.0, 70.0, None, 'cloudy'],
            ['2023-01-05', 'TestCity', 18.0, 75.0, 15.0, 'sunny'],
            ['invalid_date', 'TestCity', 15.0, 80.0, 20.0, 'unknown'],  # Invalid date
            ['2023-01-06', 'TestCity', 19.0, 85.0, 25.0, 'sunny'],
        ]
        
        cls.df = pd.DataFrame(
            cls.test_data,
            columns=[
                'date', 'city', 'temperature_celsius',
                'humidity_percent', 'wind_speed_kph', 'weather_condition'
            ]
        )
        
        # Save test data to CSV
        cls.test_csv = cls.test_dir / 'test_weather_data.csv'
        cls.df.to_csv(cls.test_csv, index=False)
        
        # Initialize pipeline with test data
        cls.output_dir = cls.test_dir / 'outputs'
        cls.pipeline = WeatherDataPipeline(cls.test_csv, cls.output_dir)
    
    def test_load_data(self):
        """Test loading data from CSV file."""
        self.pipeline.load_data()
        self.assertIsNotNone(self.pipeline.df)
        self.assertEqual(len(self.pipeline.df), 7)  # All rows should be loaded initially
    
    def test_clean_data(self):
        """Test data cleaning process."""
        self.pipeline.load_data()
        self.pipeline.clean_data()
        
        # Check that rows with invalid dates are removed
        self.assertEqual(len(self.pipeline.df), 6)  # Should have 6 valid rows (7 total - 1 invalid date)
        
        # Check that missing values are filled
        self.assertFalse(self.pipeline.df['temperature_celsius'].isna().any())
        self.assertFalse(self.pipeline.df['humidity_percent'].isna().any())
        self.assertFalse(self.pipeline.df['wind_speed_kph'].isna().any())
        
        # Check that weather_condition is standardized
        self.assertTrue(all(isinstance(x, str) for x in self.pipeline.df['weather_condition']))
        
        # Check that 'unknown' weather conditions are removed
        self.assertNotIn('unknown', self.pipeline.df['weather_condition'].values)
    
    def test_transform_data(self):
        """Test data transformation."""
        self.pipeline.load_data()
        self.pipeline.clean_data()
        self.pipeline.transform_data()
        
        # Check that temperature_fahrenheit is calculated correctly
        if 'temperature_fahrenheit' in self.pipeline.df.columns:
            expected_temp = (self.pipeline.df['temperature_celsius'] * 9/5) + 32
            pd.testing.assert_series_equal(
                self.pipeline.df['temperature_fahrenheit'].round(2),
                expected_temp.round(2),
                check_names=False
            )
        
        # Check that date components are added
        self.assertIn('year', self.pipeline.df.columns)
        self.assertIn('month', self.pipeline.df.columns)
        self.assertIn('day', self.pipeline.df.columns)
    
    def test_analyze_data(self):
        """Test data analysis."""
        self.pipeline.load_data()
        self.pipeline.clean_data()
        self.pipeline.transform_data()
        results = self.pipeline.analyze_data()
        
        # Check that all expected analysis results are present
        self.assertIn('basic_stats', results)
        self.assertIn('city_stats', results)
        self.assertIn('weather_freq', results)
        self.assertIn('top_cities', results)
        
        # Check that we have the expected number of cities
        self.assertEqual(len(results['top_cities']), 1)  # Only TestCity in test data
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test files."""
        # Remove test files
        if cls.test_csv.exists():
            cls.test_csv.unlink()
        
        # Remove output directory if it exists
        if cls.output_dir.exists():
            for file in cls.output_dir.glob('*'):
                file.unlink()
            cls.output_dir.rmdir()
        
        # Remove test directory if empty
        if cls.test_dir.exists() and not any(cls.test_dir.iterdir()):
            cls.test_dir.rmdir()


if __name__ == '__main__':
    unittest.main()
