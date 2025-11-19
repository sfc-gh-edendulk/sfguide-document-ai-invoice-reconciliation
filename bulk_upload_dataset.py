#!/usr/bin/env python3
"""
Bulk Upload Script for Dataset_colored_IBAN
============================================

This script uploads all PDF files from the Dataset_colored_IBAN directory
to the Snowflake DOC_AI_STAGE for processing by the AI invoice applications.

Usage:
    python bulk_upload_dataset.py

Features:
- Bulk uploads all 1000+ invoice PDFs
- Progress tracking and error handling
- Validates file uploads
- Refreshes stage after upload
- Resume capability for interrupted uploads
"""

import os
import io
import time
from pathlib import Path
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import streamlit as st

# Configuration
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"
STAGE_NAME = "DOC_AI_STAGE"

# Source directory (update this to your Dataset_colored_IBAN path)
DATASET_DIR = "/Users/edendulk/code/sfguide-document-ai-invoice-reconciliation/Dataset_colored_IBAN"

def get_snowflake_session():
    """Get Snowflake session - try active session first, then connection parameters"""
    try:
        # Try to get active session (if running in Streamlit/Snowpark)
        session = get_active_session()
        print("✅ Using active Snowflake session")
        return session
    except:
        try:
            # If no active session, you can set up connection parameters here
            print("⚠️  No active session found. Please run this script in a Snowflake environment")
            print("   or add your connection parameters below.")
            
            # Uncomment and fill in your connection details if needed:
            # connection_params = {
            #     "account": "your_account",
            #     "user": "your_username", 
            #     "password": "your_password",
            #     "warehouse": "DOC_AI_QS_WH",
            #     "database": DB_NAME,
            #     "schema": SCHEMA_NAME
            # }
            # session = Session.builder.configs(connection_params).create()
            
            return None
        except Exception as e:
            print(f"❌ Failed to create Snowflake session: {e}")
            return None

def upload_file_to_stage(session, file_path, stage_file_name):
    """Upload a single file to the Snowflake stage"""
    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()
        
        # Upload to stage
        session.file.put_stream(
            io.BytesIO(file_bytes), 
            f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{stage_file_name}", 
            overwrite=True, 
            auto_compress=False
        )
        return True
    except Exception as e:
        print(f"❌ Error uploading {stage_file_name}: {e}")
        return False

def get_uploaded_files(session):
    """Get list of files already uploaded to the stage"""
    try:
        result = session.sql(f"LIST @{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}").collect()
        uploaded_files = [row['name'].split('/')[-1] for row in result]
        return set(uploaded_files)
    except Exception as e:
        print(f"⚠️  Could not get uploaded files list: {e}")
        return set()

def bulk_upload_dataset():
    """Main function to bulk upload all PDF files from Dataset_colored_IBAN"""
    
    # Check if directory exists
    if not os.path.exists(DATASET_DIR):
        print(f"❌ Dataset directory not found: {DATASET_DIR}")
        print("   Please update the DATASET_DIR path in the script")
        return False
    
    # Get Snowflake session
    session = get_snowflake_session()
    if not session:
        return False
    
    # Get list of PDF files to upload
    pdf_files = list(Path(DATASET_DIR).glob("*.pdf"))
    total_files = len(pdf_files)
    
    if total_files == 0:
        print(f"❌ No PDF files found in {DATASET_DIR}")
        return False
    
    print(f"🔍 Found {total_files} PDF files in dataset directory")
    
    # Check which files are already uploaded (for resume capability)
    uploaded_files = get_uploaded_files(session)
    files_to_upload = [f for f in pdf_files if f.name not in uploaded_files]
    
    if len(files_to_upload) == 0:
        print("✅ All files already uploaded!")
        return True
    
    print(f"📤 Uploading {len(files_to_upload)} files ({total_files - len(files_to_upload)} already uploaded)")
    
    # Upload files with progress tracking
    success_count = 0
    error_count = 0
    
    for i, file_path in enumerate(files_to_upload, 1):
        file_name = file_path.name
        
        print(f"📤 [{i:4d}/{len(files_to_upload):4d}] Uploading {file_name}...", end=" ")
        
        if upload_file_to_stage(session, file_path, file_name):
            success_count += 1
            print("✅")
        else:
            error_count += 1
            print("❌")
        
        # Progress update every 50 files
        if i % 50 == 0:
            print(f"   Progress: {i}/{len(files_to_upload)} files ({(i/len(files_to_upload)*100):.1f}%)")
    
    print(f"\n📊 Upload Summary:")
    print(f"   ✅ Successfully uploaded: {success_count}")
    print(f"   ❌ Failed uploads: {error_count}")
    print(f"   📁 Total files in stage: {success_count + len(uploaded_files)}")
    
    # Refresh the stage to detect new files
    if success_count > 0:
        print("🔄 Refreshing stage to detect new files...")
        try:
            session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
            print("✅ Stage refreshed successfully")
        except Exception as e:
            print(f"⚠️  Warning: Could not refresh stage: {e}")
    
    return error_count == 0

def verify_upload():
    """Verify the upload was successful"""
    session = get_snowflake_session()
    if not session:
        return
    
    try:
        # Check files in stage
        stage_files = session.sql(f"LIST @{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}").collect()
        stage_count = len(stage_files)
        
        # Check local files
        local_files = list(Path(DATASET_DIR).glob("*.pdf"))
        local_count = len(local_files)
        
        print(f"\n📊 Verification Results:")
        print(f"   📁 Local PDF files: {local_count}")
        print(f"   ☁️  Files in Snowflake stage: {stage_count}")
        
        if stage_count >= local_count:
            print("✅ Upload verification successful!")
        else:
            print(f"⚠️  {local_count - stage_count} files may be missing from stage")
            
        # Show recent files
        print(f"\n📋 Recent files in stage:")
        for i, file_info in enumerate(stage_files[-10:], 1):
            file_name = file_info['name'].split('/')[-1]
            print(f"   {i:2d}. {file_name}")
        
        if stage_count > 10:
            print(f"   ... and {stage_count - 10} more files")
            
    except Exception as e:
        print(f"❌ Error during verification: {e}")

if __name__ == "__main__":
    print("🚀 Bulk Upload Script for Dataset_colored_IBAN")
    print("=" * 50)
    
    # Run the bulk upload
    success = bulk_upload_dataset()
    
    if success:
        print("\n🎉 Bulk upload completed successfully!")
        verify_upload()
        
        print("\n🔗 Next Steps:")
        print("   1. Run any of the enhanced invoice apps:")
        print("      - streamlit run sis_basic_invoice_app.py")
        print("      - streamlit run sis_enhanced_invoice_app.py") 
        print("      - streamlit run cortex_enhanced_app.py")
        print("   2. Your 1000+ invoices are now available for AI analysis")
        print("   3. Test fraud detection, categorization, and AI assistant features")
        
    else:
        print("\n❌ Upload completed with errors. Check the output above.")
        print("   You can re-run this script to resume uploading remaining files.") 