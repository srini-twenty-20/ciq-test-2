-- TTB Pipeline Supabase Tables
-- Run this SQL in your Supabase SQL editor to create the required tables

-- Reference Tables
CREATE TABLE IF NOT EXISTS ttb_product_class_types (
    code VARCHAR PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ttb_origin_codes (
    code VARCHAR PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Date Dimension Table (5,844 records, static)
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    quarter_name VARCHAR(5),
    season VARCHAR(10),
    days_from_epoch INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Company Dimension Table (partitioned by date)
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id BIGINT PRIMARY KEY,
    business_name TEXT,
    mailing_address TEXT,
    phone VARCHAR(50),
    email VARCHAR(255),
    fax VARCHAR(50),
    first_seen_date DATE,
    last_seen_date DATE,
    total_applications INTEGER,
    data_quality_score DECIMAL(5,3),
    source_ttb_ids TEXT[], -- PostgreSQL array
    partition_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Product Dimension Table (partitioned by date)
CREATE TABLE IF NOT EXISTS dim_products (
    product_id BIGINT PRIMARY KEY,
    brand_name TEXT,
    fanciful_name TEXT,
    product_description TEXT,
    class_type_code VARCHAR(100),
    origin_code VARCHAR(100),
    product_category VARCHAR(50),
    grape_varietals TEXT,
    wine_appellation TEXT,
    alcohol_content DECIMAL(5,2),
    net_contents VARCHAR(100),
    first_seen_date DATE,
    last_seen_date DATE,
    total_labels INTEGER,
    data_quality_score DECIMAL(5,3),
    source_ttb_ids TEXT[],
    partition_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Product Facts Table (main analytical table)
CREATE TABLE IF NOT EXISTS fact_products (
    product_fact_id BIGINT PRIMARY KEY,
    ttb_id VARCHAR(50) NOT NULL,
    company_id BIGINT REFERENCES dim_companies(company_id),
    product_id BIGINT REFERENCES dim_products(product_id),
    filing_date_id INTEGER REFERENCES dim_dates(date_id),
    approval_date_id INTEGER REFERENCES dim_dates(date_id),
    expiration_date_id INTEGER REFERENCES dim_dates(date_id),
    final_quality_score DECIMAL(5,3),
    data_completeness_score DECIMAL(5,3),
    days_to_approval INTEGER,
    has_certificate_data BOOLEAN,
    has_cola_detail_data BOOLEAN,
    class_type_code VARCHAR(100),
    origin_code VARCHAR(100),
    product_category VARCHAR(50),
    status VARCHAR(50),
    serial_number VARCHAR(50),
    vendor_code VARCHAR(50),
    filing_date DATE,
    approval_date DATE,
    expiration_date DATE,
    partition_date DATE,
    fact_creation_timestamp TIMESTAMPTZ,
    source_extraction_timestamp TIMESTAMPTZ,
    source_cleaning_timestamp TIMESTAMPTZ,
    source_structuring_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Certificate Facts Table (if needed later)
CREATE TABLE IF NOT EXISTS fact_certificates (
    certificate_fact_id BIGINT PRIMARY KEY,
    ttb_id VARCHAR(50) NOT NULL,
    company_id BIGINT REFERENCES dim_companies(company_id),
    product_id BIGINT REFERENCES dim_products(product_id),
    filing_date_id INTEGER REFERENCES dim_dates(date_id),
    approval_date_id INTEGER REFERENCES dim_dates(date_id),
    expiration_date_id INTEGER REFERENCES dim_dates(date_id),
    final_quality_score DECIMAL(5,3),
    data_completeness_score DECIMAL(5,3),
    days_to_approval INTEGER,
    status VARCHAR(50),
    serial_number VARCHAR(50),
    vendor_code VARCHAR(50),
    filing_date DATE,
    approval_date DATE,
    expiration_date DATE,
    partition_date DATE,
    fact_creation_timestamp TIMESTAMPTZ,
    source_extraction_timestamp TIMESTAMPTZ,
    source_cleaning_timestamp TIMESTAMPTZ,
    source_structuring_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_fact_products_company_id ON fact_products(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_products_product_id ON fact_products(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_products_approval_date ON fact_products(approval_date);
CREATE INDEX IF NOT EXISTS idx_fact_products_partition_date ON fact_products(partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_products_ttb_id ON fact_products(ttb_id);

CREATE INDEX IF NOT EXISTS idx_dim_companies_partition_date ON dim_companies(partition_date);
CREATE INDEX IF NOT EXISTS idx_dim_companies_business_name ON dim_companies(business_name);

CREATE INDEX IF NOT EXISTS idx_dim_products_partition_date ON dim_products(partition_date);
CREATE INDEX IF NOT EXISTS idx_dim_products_brand_name ON dim_products(brand_name);
CREATE INDEX IF NOT EXISTS idx_dim_products_class_type ON dim_products(class_type_code);
CREATE INDEX IF NOT EXISTS idx_dim_products_origin ON dim_products(origin_code);

-- Foreign Key Indexes
CREATE INDEX IF NOT EXISTS idx_fact_certificates_company_id ON fact_certificates(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_product_id ON fact_certificates(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_approval_date ON fact_certificates(approval_date);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_partition_date ON fact_certificates(partition_date);

-- Comments for documentation
COMMENT ON TABLE ttb_product_class_types IS 'TTB reference data for product class and type codes';
COMMENT ON TABLE ttb_origin_codes IS 'TTB reference data for origin codes (countries/regions)';
COMMENT ON TABLE dim_dates IS 'Date dimension table covering 2015-2030 for TTB analytics';
COMMENT ON TABLE dim_companies IS 'Company dimension with deduplication and data quality scores';
COMMENT ON TABLE dim_products IS 'Product dimension with brand, category, and quality information';
COMMENT ON TABLE fact_products IS 'Main fact table for TTB product applications and approvals';
COMMENT ON TABLE fact_certificates IS 'Fact table for TTB certificate data (future use)';

-- Enable Row Level Security (optional - for multi-tenant scenarios)
-- ALTER TABLE ttb_product_class_types ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE ttb_origin_codes ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dim_dates ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dim_companies ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dim_products ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fact_products ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fact_certificates ENABLE ROW LEVEL SECURITY;