# Data Directory

This directory contains sample data, outputs, and development artifacts.

## 📁 Structure

### Sample Outputs (`sample_outputs/`)
Example outputs from TTB parsing operations:
- `beer_parsing_output.json` - Sample beer parsing results
- `certificate_parsing_output.json` - Sample certificate parsing results
- `cola_parsing_output.json` - Sample COLA parsing results

## 📋 Usage

These files serve as:
- **Reference Examples**: See expected output formats
- **Development Testing**: Compare against actual parsing results
- **Documentation**: Understand data structures and formats

## ⚠️ Note

This directory contains sample/development data only. Production data is stored in S3 and managed through the Dagster pipeline.