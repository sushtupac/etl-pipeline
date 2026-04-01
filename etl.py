import boto3
import csv
import json
import io
from datetime import datetime
from collections import defaultdict

ENDPOINT = 'http://localhost:4566'
REGION = 'us-east-1'

RAW_BUCKET = 'sales-raw-data'
PROCESSED_BUCKET = 'sales-processed-data'
REPORTS_BUCKET = 'sales-reports'
RAW_KEY = 'incoming/sales_2024_01.csv'

def get_client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )


def extract(s3):
    print("EXTRACT — reading raw CSV from S3...")
    response = s3.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)
    content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))
    records = list(reader)
    print(f"  Read {len(records)} records from s3://{RAW_BUCKET}/{RAW_KEY}")
    return records


def transform(records):
    print("\nTRANSFORM — cleaning and enriching data...")
    
    cleaned = []
    skipped = 0
    
    for row in records:
        try:
            # Clean customer name — title case
            customer = row['customer_name'].strip().title()
            
            # Normalize status — lowercase
            status = row['status'].strip().lower()
            
            # Parse and validate numbers
            quantity = int(row['quantity'].strip())
            price = float(row['price'].strip())
            
            # Calculate total value
            total_value = round(quantity * price, 2)
            
            # Parse date
            order_date = datetime.strptime(
                row['order_date'].strip(), '%Y-%m-%d'
            )
            
            # Build clean record
            clean_record = {
                'order_id': row['order_id'].strip(),
                'customer_name': customer,
                'product': row['product'].strip(),
                'quantity': quantity,
                'unit_price': price,
                'total_value': total_value,
                'order_date': order_date.strftime('%Y-%m-%d'),
                'year': order_date.year,
                'month': order_date.month,
                'day': order_date.day,
                'status': status,
                'processed_at': datetime.utcnow().isoformat()
            }
            
            cleaned.append(clean_record)
        
        except Exception as e:
            print(f"  Skipping row {row.get('order_id', '?')}: {e}")
            skipped += 1
    
    print(f"  Cleaned {len(cleaned)} records, skipped {skipped}")
    return cleaned


def load(s3, records):
    print("\nLOAD — writing processed data to S3...")
    
    # Group records by date for partitioning
    partitions = defaultdict(list)
    for record in records:
        key = f"year={record['year']}/month={record['month']:02d}/day={record['day']:02d}"
        partitions[key].append(record)
    
    uploaded = 0
    for partition, rows in partitions.items():
        # Write each partition as newline-delimited JSON
        ndjson = '\n'.join(json.dumps(row) for row in rows)
        s3_key = f"sales/{partition}/data.json"
        
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=s3_key,
            Body=ndjson.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  Uploaded {len(rows)} records to s3://{PROCESSED_BUCKET}/{s3_key}")
        uploaded += 1
    
    print(f"  Created {uploaded} partitions")
    return partitions


def generate_report(s3, records):
    print("\nREPORT — generating pipeline summary...")
    
    total_orders = len(records)
    total_revenue = sum(r['total_value'] for r in records)
    
    # Revenue by product
    by_product = defaultdict(float)
    for r in records:
        by_product[r['product']] += r['total_value']
    
    # Orders by status
    by_status = defaultdict(int)
    for r in records:
        by_status[r['status']] += 1
    
    # Top customer
    by_customer = defaultdict(float)
    for r in records:
        by_customer[r['customer_name']] += r['total_value']
    top_customer = max(by_customer, key=by_customer.get)
    
    report = {
        'pipeline_run': datetime.utcnow().isoformat(),
        'source': f's3://{RAW_BUCKET}/{RAW_KEY}',
        'summary': {
            'total_orders': total_orders,
            'total_revenue': round(total_revenue, 2),
            'average_order_value': round(total_revenue / total_orders, 2),
        },
        'revenue_by_product': dict(sorted(
            by_product.items(), key=lambda x: x[1], reverse=True
        )),
        'orders_by_status': dict(by_status),
        'top_customer': {
            'name': top_customer,
            'total_spent': round(by_customer[top_customer], 2)
        }
    }
    
    report_key = f"pipeline_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=REPORTS_BUCKET,
        Key=report_key,
        Body=json.dumps(report, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    
    print(f"  Report saved to s3://{REPORTS_BUCKET}/{report_key}")
    print(f"\n  Total orders:   {report['summary']['total_orders']}")
    print(f"  Total revenue:  ${report['summary']['total_revenue']:,.2f}")
    print(f"  Avg order value: ${report['summary']['average_order_value']:,.2f}")
    print(f"  Top customer:   {top_customer} (${report['summary']['total_revenue']:,.2f})")
    print(f"\n  Revenue by product:")
    for product, revenue in report['revenue_by_product'].items():
        print(f"    {product:<12} ${revenue:,.2f}")
    print(f"\n  Orders by status:")
    for status, count in report['orders_by_status'].items():
        print(f"    {status:<12} {count} orders")
    
    return report


def run_pipeline():
    print("=" * 50)
    print("ETL PIPELINE STARTING")
    print("=" * 50)
    
    s3 = get_client('s3')
    
    # Extract
    raw_records = extract(s3)
    
    # Transform
    clean_records = transform(raw_records)
    
    # Load
    load(s3, clean_records)
    
    # Report
    generate_report(s3, clean_records)
    
    print("\n" + "=" * 50)
    print("PIPELINE COMPLETE")
    print("=" * 50)


if __name__ == '__main__':
    run_pipeline()