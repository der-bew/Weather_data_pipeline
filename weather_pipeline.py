#!/usr/bin/env python3
"""
Weather Data Processing Pipeline

This script processes weather data from a CSV file, performs cleaning and transformation,
and saves the results to output files.
"""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


class WeatherDataPipeline:
    """A pipeline for processing weather data."""

    def __init__(self, input_file, output_dir="outputs"):
        """Initialize the pipeline with input and output paths.

        Args:
            input_file (str): Path to the input CSV file
            output_dir (str): Directory to save output files
        """
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.df = None
        self.required_columns = [
            "date",
            "city",
            "temperature_celsius",
            "humidity_percent",
            "wind_speed_kph",
            "weather_condition",
        ]

    def load_data(self):
        """Load data from CSV file and handle date parsing."""
        try:
            # Read the CSV file
            self.df = pd.read_csv(
                self.input_file,
                parse_dates=["date"],
                dayfirst=True,  # Handle DD/MM/YYYY format
                infer_datetime_format=True,
                na_values=["", " ", "NA", "N/A", "NaN", "None", "unknown", "Unknown"],
            )

            # Check for required columns
            missing_cols = [
                col for col in self.required_columns if col not in self.df.columns
            ]
            if missing_cols:
                raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

            return self

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise

    def clean_data(self):
        """Clean the weather data."""
        if self.df is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        # Convert date column to datetime, coerce errors to NaT
        self.df['date'] = pd.to_datetime(self.df['date'], errors='coerce')
        
        # Drop rows with missing dates (including those that couldn't be parsed)
        initial_count = len(self.df)
        self.df = self.df.dropna(subset=['date']).copy()
        dropped_count = initial_count - len(self.df)
        if dropped_count > 0:
            print(f"Dropped {dropped_count} rows with invalid/missing dates")
        
        # Standardize weather conditions (convert to lowercase and strip whitespace)
        if 'weather_condition' in self.df.columns:
            self.df['weather_condition'] = self.df['weather_condition'].astype(str).str.lower().str.strip()
        
        # Filter out 'unknown' weather conditions
        if 'weather_condition' in self.df.columns:
            initial_count = len(self.df)
            self.df = self.df[~self.df['weather_condition'].isin(['unknown', ''])]
            dropped_count = initial_count - len(self.df)
            if dropped_count > 0:
                print(f"Dropped {dropped_count} rows with unknown/empty weather conditions")
        
        # Fill missing numeric values with city-wise medians
        numeric_cols = ['temperature_celsius', 'humidity_percent', 'wind_speed_kph']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = self.df.groupby('city')[col].transform(
                    lambda x: x.fillna(x.median())
                )
        
        # Fill any remaining missing values with column medians
        for col in numeric_cols:
            if col in self.df.columns and self.df[col].isna().any():
                self.df[col] = self.df[col].fillna(self.df[col].median())
        
        return self

    def transform_data(self):
        """Transform the weather data."""
        if self.df is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        # Ensure date is in datetime format (should already be from clean_data, but just in case)
        self.df['date'] = pd.to_datetime(self.df['date'], errors='coerce')
        
        # Sort by date
        self.df = self.df.sort_values('date')
        
        # Extract date components
        self.df['year'] = self.df['date'].dt.year
        self.df['month'] = self.df['date'].dt.month
        self.df['day'] = self.df['date'].dt.day
        
        # Convert temperature to Fahrenheit if celsius column exists
        if 'temperature_celsius' in self.df.columns:
            self.df['temperature_fahrenheit'] = (self.df['temperature_celsius'] * 9/5) + 32
        
        return self

    def analyze_data(self):
        """Perform analysis on the weather data."""
        if self.df is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        analysis_results = {}

        # Basic statistics
        numeric_cols = [
            "temperature_celsius",
            "humidity_percent",
            "wind_speed_kph",
            "temperature_fahrenheit",
        ]
        analysis_results["basic_stats"] = self.df[numeric_cols].describe().round(2)

        # City-wise statistics
        analysis_results["city_stats"] = (
            self.df.groupby("city")[numeric_cols]
            .agg(["mean", "median", "min", "max"])
            .round(2)
        )

        # Weather condition frequency
        analysis_results["weather_freq"] = (
            self.df["weather_condition"].value_counts().to_frame("count")
        )

        # Top 5 cities by average temperature
        top_cities = (
            self.df.groupby("city")["temperature_celsius"].mean().nlargest(5).round(2)
        )
        analysis_results["top_cities"] = top_cities

        return analysis_results

    def generate_report(self, analysis_results):
        """Generate a markdown report with analysis results."""
        report = "# Weather Data Analysis Report\n\n"

        # Top 5 cities by temperature
        if "top_cities" in analysis_results:
            report += "## Top 5 Warmest Cities\n\n"
            report += "| Rank | City | Average Temperature (°C) |\n"
            report += "|------|------|--------------------------|\n"
            for i, (city, temp) in enumerate(analysis_results["top_cities"].items(), 1):
                report += f"| {i} | {city} | {temp}°C |\n"
            report += "\n"

        # Save report to file
        report_path = self.output_dir / "top_cities_report.md"
        with open(report_path, "w") as f:
            f.write(report)
        print(f"Report saved to {report_path}")

        return report_path

    def plot_temperature_by_city(self):
        """Create a bar plot of average temperature by city."""
        plt.figure(figsize=(12, 6))

        # Calculate average temperature by city
        avg_temp = (
            self.df.groupby("city")["temperature_celsius"]
            .mean()
            .sort_values(ascending=False)
        )

        # Create bar plot
        ax = sns.barplot(x=avg_temp.index, y=avg_temp.values, palette="coolwarm")

        # Customize plot
        plt.title("Average Temperature by City", fontsize=16)
        plt.xlabel("City", fontsize=12)
        plt.ylabel("Average Temperature (°C)", fontsize=12)
        plt.xticks(rotation=45, ha="right")

        # Add value labels on top of bars
        for i, v in enumerate(avg_temp.values):
            ax.text(i, v + 0.1, f"{v:.1f}°C", ha="center")

        # Save plot
        plot_path = self.output_dir / "avg_temperature_by_city.png"
        plt.tight_layout()
        plt.savefig(plot_path, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"Plot saved to {plot_path}")
        return plot_path

    def save_results(self, analysis_results):
        """Save the processed data and analysis results."""
        if self.df is None:
            raise ValueError("No data to save. Process data first.")

        # Save processed data with all columns
        processed_file = self.output_dir / "transformed_weather_data.csv"
        self.df.to_csv(processed_file, index=False)
        print(f"Processed data saved to {processed_file}")

        # Save analysis results
        for name, result in analysis_results.items():
            if name != "top_cities":  # Skip top_cities as it's handled in the report
                output_file = self.output_dir / f"{name}.csv"
                result.to_csv(output_file)
                print(f"{name} saved to {output_file}")

        # Generate and save report
        self.generate_report(analysis_results)

        # Create and save temperature plot
        self.plot_temperature_by_city()

    def run_pipeline(self):
        """Run the complete data processing pipeline."""
        print("Starting weather data processing pipeline...")

        try:
            # Execute pipeline steps
            self.load_data()
            self.clean_data()
            self.transform_data()
            analysis_results = self.analyze_data()

            # Save results
            self.save_results(analysis_results)
            print("\nPipeline completed successfully!")

            return True

        except Exception as e:
            print(f"\nError in pipeline: {str(e)}")
            return False


def main():
    """Main function to run the weather data pipeline."""
    # Define file paths
    data_dir = Path(__file__).parent / "data"
    input_file = data_dir / "weather_data.csv"
    output_dir = Path(__file__).parent / "outputs"

    # Create and run pipeline
    pipeline = WeatherDataPipeline(input_file, output_dir)
    success = pipeline.run_pipeline()

    if not success:
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
