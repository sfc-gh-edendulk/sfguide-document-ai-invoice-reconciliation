#!/bin/bash

# =================================================================
# Bulk Upload Script using SnowCLI for Dataset_colored_IBAN
# =================================================================

echo "🚀 Starting bulk upload of Dataset_colored_IBAN using SnowCLI..."
echo "=================================================================="

# Configuration
DATASET_DIR="Dataset_colored_IBAN"
STAGE_NAME="@DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE"

# Check if we're in the right directory
if [ ! -d "$DATASET_DIR" ]; then
    echo "❌ Error: $DATASET_DIR directory not found!"
    echo "   Make sure you're running this from the project root directory"
    echo "   Current directory: $(pwd)"
    exit 1
fi

# Check if snowcli is available
if ! command -v snow &> /dev/null; then
    echo "❌ Error: snowcli not found!"
    echo "   Please install snowcli first:"
    echo "   pip install snowflake-cli-labs"
    echo "   or: pipx install snowflake-cli-labs"
    exit 1
fi

# Check snowcli connection
echo "🔍 Checking SnowCLI connection..."
if ! snow connection test &> /dev/null; then
    echo "❌ Error: SnowCLI connection failed!"
    echo "   Please configure your connection first:"
    echo "   snow connection add"
    echo "   or check: snow connection list"
    exit 1
else
    echo "✅ SnowCLI connection verified"
fi

# Count total PDF files
total_files=$(find "$DATASET_DIR" -name "*.pdf" | wc -l)
echo "🔍 Found $total_files PDF files in $DATASET_DIR"

if [ "$total_files" -eq 0 ]; then
    echo "❌ No PDF files found in $DATASET_DIR"
    exit 1
fi

# Check current files in stage
echo "🔍 Checking existing files in Snowflake stage..."
existing_files=$(snow stage list "$STAGE_NAME" --format json 2>/dev/null | jq -r '.[].name' | wc -l 2>/dev/null || echo "0")
echo "📁 Stage currently has $existing_files files"

# Create a temporary file list for batch processing
temp_file_list=$(mktemp)
find "$DATASET_DIR" -name "*.pdf" > "$temp_file_list"

echo ""
echo "🚀 Starting bulk upload..."
echo "   This may take several minutes for $total_files files..."
echo "   Progress will be shown every 50 files"
echo ""

# Upload files in batches
uploaded_count=0
error_count=0
batch_size=50

while IFS= read -r file_path; do
    file_name=$(basename "$file_path")
    
    # Upload file using snowcli
    if snow stage put "$file_path" "$STAGE_NAME" --overwrite --quiet > /dev/null 2>&1; then
        ((uploaded_count++))
        echo -n "."
    else
        ((error_count++))
        echo -n "❌"
        echo " Error uploading: $file_name" >&2
    fi
    
    # Progress update every batch_size files
    if [ $((uploaded_count % batch_size)) -eq 0 ] && [ $uploaded_count -gt 0 ]; then
        echo ""
        echo "   Progress: $uploaded_count/$total_files files uploaded ($(( (uploaded_count * 100) / total_files ))%)"
        
        # Refresh stage periodically
        echo "   🔄 Refreshing stage..."
        snow sql --query "ALTER STAGE $STAGE_NAME REFRESH" --quiet > /dev/null 2>&1
    fi
    
done < "$temp_file_list"

echo ""
echo ""

# Final refresh
echo "🔄 Final stage refresh..."
snow sql --query "ALTER STAGE $STAGE_NAME REFRESH" --quiet

# Cleanup
rm "$temp_file_list"

# Summary
echo ""
echo "📊 Upload Summary:"
echo "   ✅ Successfully uploaded: $uploaded_count"
echo "   ❌ Failed uploads: $error_count"
echo "   📁 Total processed: $((uploaded_count + error_count))"

# Verify upload
echo ""
echo "🔍 Verifying upload..."
final_stage_count=$(snow stage list "$STAGE_NAME" --format json 2>/dev/null | jq length 2>/dev/null || echo "unknown")
echo "   ☁️  Files now in stage: $final_stage_count"

if [ "$final_stage_count" != "unknown" ] && [ "$final_stage_count" -ge "$total_files" ]; then
    echo "   ✅ Upload verification successful!"
else
    echo "   ⚠️  Upload verification inconclusive"
fi

echo ""
echo "=================================================================="

if [ $error_count -eq 0 ]; then
    echo "🎉 Bulk upload completed successfully!"
    echo ""
    echo "🔗 Next Steps:"
    echo "   1. Run Streamlit apps to analyze your enhanced dataset:"
    echo "      streamlit run sis_basic_invoice_app.py"
    echo "      streamlit run sis_enhanced_invoice_app.py"
    echo "      streamlit run cortex_enhanced_app.py"
    echo ""
    echo "   2. Deploy to Streamlit in Snowflake for best experience"
    echo "   3. Test AI features:"
    echo "      - Fraud detection on 1000+ invoices"
    echo "      - AI categorization and insights"
    echo "      - Advanced analytics with large dataset"
    echo ""
    echo "   4. Initialize AI features in Snowflake:"
    echo "      snow sql --filename cortex_enhancements.sql"
    echo "      snow sql --query \"CALL SP_BATCH_FRAUD_ASSESSMENT()\""
    echo "      snow sql --query \"CALL SP_AI_CATEGORIZE_INVOICES()\""
else
    echo "❌ Upload completed with $error_count errors"
    echo "   Check the output above for details"
    echo "   You can re-run this script to retry failed uploads"
fi

echo "==================================================================" 